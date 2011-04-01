import os
import subprocess
from threading import Thread
from dvdtitlestream import DVDTitleStream

import logging
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import lrucache

import config
import metadata
import virtualdvd

logger = logging.getLogger('pyTivo.dvdvideo.vobstream')

info_cache = lrucache.LRUCache(1000)
mpgcat_procs = {}
reapers = {}

BLOCKSIZE = 512 * 1024
MAXBLOCKS = 2
TIMEOUT = 600
USE_FFMPEG = True

# XXX BIG HACK
# subprocess is broken for me on windows so super hack
def patchSubprocess():
    o = subprocess.Popen._make_inheritable

    def _make_inheritable(self, handle):
        if not handle: return subprocess.GetCurrentProcess()
        return o(self, handle)

    subprocess.Popen._make_inheritable = _make_inheritable

mswindows = (sys.platform == "win32")
if mswindows:
    patchSubprocess()

def debug(msg):
    if type(msg) == str:
        try:
            msg = msg.decode('utf8')
        except:
            if sys.platform == 'darwin':
                msg = msg.decode('macroman')
            else:
                msg = msg.decode('iso8859-1')
    logger.debug(msg)
    
def WriteSectorStreamToSubprocess( fhin, sub, event, blocksize ):

    # Write all the data till either end is closed or done
    while not event.isSet():
        
        # Read in the block and escape if we got nothing
        data = fhin.read( blocksize )
        if len(data) == 0:
            break
        
        if sub.poll() != None and sub.stdin != None:
            break

        # Write the data and flush it
        try:
            sub.stdin.write( data )
            sub.stdin.flush()
        except IOError:
            break
        
        # We got less data so we must be at the end
        if len(data) < blocksize:
            break
    
    # Close the input if it's not already closed
    if not fhin.closed:
        fhin.close()
    
    # Close the output if it's not already closed
    if sub.stdin != None and not sub.stdin.closed:
        sub.stdin.close()

def vobstream(isQuery, inFile, outFile, tsn=''):
    settings = {'TBD': 'TBD'}

    if isQuery:
        return settings

    ffmpeg_path = config.get_bin('ffmpeg')
    
    dvd = virtualdvd.VirtualDVD( inFile )
    title = dvd.FileTitle()
    ts = DVDTitleStream( title.Stream() )
    
    vinfo = video_info( inFile )
    vmap = vinfo['mapVideo'].replace( '.', ':' )
    amap = vinfo['mapAudio'].replace( '.', ':' )
    
    if USE_FFMPEG:
        sp = subprocess.Popen( [ ffmpeg_path, '-i', '-', \
            '-map', vmap, '-map', amap,
            '-acodec', 'copy', '-vcodec', 'copy', '-f', 'vob', '-' ], \
            stdout = subprocess.PIPE, \
            stdin = subprocess.PIPE, \
            bufsize = BLOCKSIZE * MAXBLOCKS )
    
        # Make an event to shutdown the thread
        sde = threading.Event()
        sde.clear()

        # Stream data to the subprocess
        t = Thread( target=WriteSectorStreamToSubprocess, args=(ts,sp,sde,BLOCKSIZE) )
        t.start()
    
    mpgcat_procs[inFile] = {'stream': ts, 'start': 0, 'end': 0, \
                            'thread': t, 'process':sp, 'event':sde, \
                            'last_read': time.time(), 'blocks': []}

    reap_process(inFile)
    transfer_blocks(inFile, outFile)

def is_resumable(inFile, offset):
    if inFile in mpgcat_procs:
        proc = mpgcat_procs[inFile]
        if proc['start'] <= offset < proc['end']:
            return True
        else:
            cleanup(inFile)
    return False

def resume_transfer(inFile, outFile, offset):
    proc = mpgcat_procs[inFile]
    offset -= proc['start']
    try:
        for block in proc['blocks']:
            length = len(block)
            if offset < length:
                if offset > 0:
                    block = block[offset:]
                outFile.write('%x\r\n' % len(block))
                outFile.write(block)
                outFile.write('\r\n')
            offset -= length
        outFile.flush()
    except Exception, msg:
        logger.info(msg)
        return
    proc['start'] = proc['end']
    proc['blocks'] = []

    transfer_blocks(inFile, outFile)

def transfer_blocks(inFile, outFile):
    proc = mpgcat_procs[inFile]
    blocks = proc['blocks']

    while True:
        try:
            if USE_FFMPEG:
                block = proc['process'].stdout.read(BLOCKSIZE)
            else:
                block = proc['stream'].read(BLOCKSIZE)
            proc['last_read'] = time.time()
        except Exception, msg:
            logger.info(msg)
            cleanup(inFile)
            break

        if not block or len(block) == 0:
            try:
                outFile.flush()
                proc['stream'].close()
            except Exception, msg:
                logger.info(msg)
            else:
                cleanup(inFile)
            break

        blocks.append(block)
        proc['end'] += len(block)
        if len(blocks) > MAXBLOCKS:
            proc['start'] += len(blocks[0])
            blocks.pop(0)

        try:
            outFile.write('%x\r\n' % len(block))
            outFile.write(block)
            outFile.write('\r\n')
        except Exception, msg:
            logger.info(msg)
            break

def reap_process(inFile):
    if inFile in mpgcat_procs:
        proc = mpgcat_procs[inFile]
        if proc['last_read'] + TIMEOUT < time.time():
            cleanup(inFile)
        
        else:
            reaper = threading.Timer(TIMEOUT, reap_process, (inFile,))
            reapers[inFile] = reaper
            reaper.start()

def cleanup(inFile):

    # Don't fear the reaper
    try:
        reapers[inFile].cancel()
        del reapers[inFile]
    except:
        pass

    if USE_FFMPEG:
        kill(mpgcat_procs[inFile]['process'])
        mpgcat_procs[inFile]['process'].wait()

    # Tell thread to break out of loop
    mpgcat_procs[inFile]['event'].set()
    mpgcat_procs[inFile]['thread'].join()
    
    del mpgcat_procs[inFile]
    
def supported_format( inFile ):
    dvd = virtualdvd.VirtualDVD( inFile )
    return dvd.Valid() and dvd.file_id != -1

def size(inFile):
    try:
        dvd = virtualdvd.VirtualDVD( inFile )
        return dvd.FileTitle().Size()
    except:
        return 0

def video_info(inFile, audio_spec = "", cache=True):
    vInfo = dict()
    try:
        mtime = os.stat(inFile).st_mtime
    except:
        mtime = 0
    
    if cache:
        if inFile in info_cache and info_cache[inFile][0] == mtime:
            debug('CACHE HIT! %s' % inFile)
            return info_cache[inFile][1]

    dvd = virtualdvd.VirtualDVD( inFile )
    if not dvd.Valid() or dvd.file_id == -1:
        debug('Not a valid dvd file')
        return dict()
    
    ffmpeg_path = config.get_bin('ffmpeg')
    
    title = dvd.FileTitle()
    sid = title.FindBestAudioStreamID( audio_spec )
    ts = DVDTitleStream( title.Stream() )
    ts.seek(0)
    
    # Make a subprocess to get the information from a stream
    proc = subprocess.Popen( [ ffmpeg_path, '-i', '-' ], \
        stdout=subprocess.PIPE, \
        stdin=subprocess.PIPE, \
        stderr=subprocess.STDOUT, \
        bufsize=BLOCKSIZE * MAXBLOCKS )
        
    # Make an event to shutdown the thread
    sde = threading.Event()
    sde.clear()

    # Stream data to the subprocess
    t = Thread( target=WriteSectorStreamToSubprocess, args=(ts,proc,sde,BLOCKSIZE) )
    t.start()

    # Readin the output from the subprocess
    output = ""
    while 1:
    
        # Don't throw on any IO errors    
        try:
            data = proc.stdout.read( BLOCKSIZE )
        except IOError:
            break
        
        # If we're blank, then the data stream is empty
        if len(data) == 0:
            break
        
        # append the output
        output += data
    
    # Shutdown the helper threads/processes    
    sde.set()
    proc.wait()
    t.join()
        
    # Close the title stream
    ts.close()
        
    #print "VOB Info:", output
    vInfo['mapAudio'] = ''

    attrs = {'container': r'Input #0, ([^,]+),',
             'vCodec': r'.*Video: ([^,]+),.*',             # video codec
             'aKbps': r'.*Audio: .+, (.+) (?:kb/s).*',     # audio bitrate
             'aCodec': r'.*Audio: ([^,]+),.*',             # audio codec
             'aFreq': r'.*Audio: .+, (.+) (?:Hz).*',       # audio frequency
             'mapVideo': r'([0-9]+\.[0-9]+).*: Video:.*',  # video mapping
             'mapAudio': r'([0-9]+\.[0-9]+)\[0x%02x\]: Audio:.*' % sid } # Audio mapping

    for attr in attrs:
        rezre = re.compile(attrs[attr])
        x = rezre.search(output)
        if x:
            #print attr, attrs[attr], x.group(1)
            vInfo[attr] = x.group(1)
        else:
            #print attr, attrs[attr], '(None)'
            if attr in ['container', 'vCodec']:
                vInfo[attr] = ''
                vInfo['Supported'] = False
            else:
                vInfo[attr] = None
            #print '***************** failed at ' + attr + ' : ' + attrs[attr]
            debug('failed at ' + attr)
            
    # Get the Pixel Aspect Ratio
    rezre = re.compile(r'.*Video: .+PAR ([0-9]+):([0-9]+) DAR [0-9:]+.*')
    x = rezre.search(output)
    if x and x.group(1) != "0" and x.group(2) != "0":
        vInfo['par1'] = x.group(1) + ':' + x.group(2)
        vInfo['par2'] = float(x.group(1)) / float(x.group(2))
    else:
        vInfo['par1'], vInfo['par2'] = None, None
 
    # Get the Display Aspect Ratio
    rezre = re.compile(r'.*Video: .+DAR ([0-9]+):([0-9]+).*')
    x = rezre.search(output)
    if x and x.group(1) != "0" and x.group(2) != "0":
        vInfo['dar1'] = x.group(1) + ':' + x.group(2)
    else:
        vInfo['dar1'] = None

    # Get the video dimensions
    rezre = re.compile(r'.*Video: .+, (\d+)x(\d+)[, ].*')
    x = rezre.search(output)
    if x:
        vInfo['vWidth'] = int(x.group(1))
        vInfo['vHeight'] = int(x.group(2))
    else:
        vInfo['vWidth'] = ''
        vInfo['vHeight'] = ''
        vInfo['Supported'] = False
        debug('failed at vWidth/vHeight')
    
    vInfo['millisecs'] = title.Time().MSecs()
    vInfo['Supported'] = True

    if cache:
        info_cache[inFile] = (mtime, vInfo)

    return vInfo

def kill(popen):
    debug('killing pid=%s' % str(popen.pid))
    if mswindows:
        win32kill(popen.pid)
    else:
        import os, signal
        for i in xrange(3):
            debug('sending SIGTERM to pid: %s' % popen.pid)
            os.kill(popen.pid, signal.SIGTERM)
            time.sleep(.5)
            if popen.poll() is not None:
                debug('process %s has exited' % popen.pid)
                break
        else:
            while popen.poll() is None:
                debug('sending SIGKILL to pid: %s' % popen.pid)
                os.kill(popen.pid, signal.SIGKILL)
                time.sleep(.5)

def win32kill(pid):
    import ctypes
    handle = ctypes.windll.kernel32.OpenProcess(1, False, pid)
    ctypes.windll.kernel32.TerminateProcess(handle, -1)
    ctypes.windll.kernel32.CloseHandle(handle)
