# Module:       dvdvideo.py
# Author:       Eric von Bayer
# Contact:      
# Date:         August 18, 2009
# Description:
#     DVD Video plugin to allow playing DVD VIDEO_TS format folders on a TiVo
#
# Copyright (c) 2009, Eric von Bayer
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#    * The names of the contributors may not be used to endorse or promote
#      products derived from this software without specific prior written
#      permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys

if sys.version_info >= (3, 0):
    raise "This plugin requires a 2.x version of python"

import cgi
import logging
import os
import re
import time
import traceback
import urllib
import zlib
import string
from UserDict import DictMixin
from datetime import datetime, timedelta
from xml.sax.saxutils import escape
from xml.dom import minidom


from Cheetah.Template import Template

import config
import metadata
import mind
import dvdfolder
import virtualdvd
import vobstream

from plugin import EncodeUnicode, Plugin, quote

logger = logging.getLogger('pyTivo.dvdvideo')

SCRIPTDIR = os.path.dirname(__file__)

CLASS_NAME = 'DVDVideo'

# Preload the templates
def tmpl(name):
    return file(os.path.join(SCRIPTDIR, 'templates', name), 'rb').read()

CONTAINER_TEMPLATE = tmpl('container.tmpl')
TVBUS_TEMPLATE = tmpl('TvBus.tmpl')
XSL_TEMPLATE = tmpl('container.xsl')

extfile = os.path.join(SCRIPTDIR, 'video.ext')
try:
    extensions = file(extfile).read().split()
except:
    extensions = None

class DVDVideo(Plugin):

    CONTENT_TYPE = 'x-container/tivo-videos'

    def pre_cache(self, full_path):
        if self.video_file_filter(self, full_path):
            vobstream.supported_format(full_path)

    def video_file_filter(self, full_path, type=None):
        if os.path.isdir(full_path):
            return True        
        if extensions:
            return os.path.splitext(full_path)[1].lower() in extensions
        else:
            return vobstream.supported_format(full_path)

    def send_file(self, handler, path, query):
        mime = 'video/mpeg'
        tsn = handler.headers.getheader('tsn', '')

        is_tivo_file = (path[-5:].lower() == '.tivo')

        if is_tivo_file and transcode.tivo_compatible(path, tsn, mime)[0]:
            mime = 'video/x-tivo-mpeg'

        if 'Format' in query:
            mime = query['Format'][0]

        needs_tivodecode = (is_tivo_file and mime == 'video/mpeg')
        compatible = True

        offset = handler.headers.getheader('Range')
        if offset:
            offset = int(offset[6:-1])  # "bytes=XXX-"

        if needs_tivodecode:
            valid = bool(config.get_bin('tivodecode') and
                         config.get_server('tivo_mak'))
        else:
            valid = True

        if valid and offset:
            valid = (not compatible and vobstream.is_resumable(path, offset))

        handler.send_response(206)
        handler.send_header('Content-Type', mime)
        handler.send_header('Connection', 'close')
#        if compatible:
#            handler.send_header('Content-Length',
#                                os.stat(path).st_size - offset)
#        else:
        handler.send_header('Transfer-Encoding', 'chunked')
        handler.end_headers()

#        if valid:
#            if compatible:
#                logger.debug('%s is tivo compatible' % path)
#                f = open(path, 'rb')
#                try:
#                    if offset:
#                        f.seek(offset)
#                    while True:
#                        block = f.read(512 * 1024)
#                        if not block:
#                            break
#                        handler.wfile.write(block)
#                except Exception, msg:
#                    logger.info(msg)
#                f.close()
#            else:
        logger.debug('%s is not tivo compatible' % path)
        if offset:
            vobstream.resume_transfer(path, handler.wfile, offset)
        else:
            vobstream.vobstream(False, path, handler.wfile, tsn)
        try:
            handler.wfile.write('0\r\n\r\n')
            handler.wfile.flush()
        except Exception, msg:
            logger.info(msg)
        logger.debug("Finished outputing video")

    def __total_items(self, full_path):
        count = 0
        try:
            for f in os.listdir(full_path):
                if f.startswith('.'):
                    continue
                f = os.path.join(full_path, f)
                if os.path.isdir(f):
                    count += 1
                elif extensions:
                    if os.path.splitext(f)[1].lower() in extensions:
                        count += 1
                elif f in vobstream.info_cache:
                    if vobstream.supported_format(f):
                        count += 1
        except:
            pass
        return count

    def __est_size(self, full_path, tsn='', mime=''):
        # Size is estimated by taking audio and video bit rate adding 2%
        return vobstream.size( full_path )

    def metadata_full(self, full_path, tsn='', mime='', audio_spec=''):
        data = {}
        
        vInfo = vobstream.video_info( full_path, audio_spec=audio_spec )

        if ((int(vInfo['vHeight']) >= 720 and
             config.getTivoHeight >= 720) or
            (int(vInfo['vWidth']) >= 1280 and
             config.getTivoWidth >= 1280)):
            data['showingBits'] = '4096'

        vdvd = virtualdvd.VirtualDVD( full_path )
        
        data['title'] = vdvd.DVDTitleName()
        
        # This expects <dvd path>/details.txt
        try:
            data.update( metadata.from_text( os.path.join( vdvd.Path(), "details" ) ) )
            ratings = {'TV-Y7': 'x1', 'TV-Y': 'x2', 'TV-G': 'x3', \
               'TV-PG': 'x4', 'TV-14': 'x5', 'TV-MA': 'x6', \
               'Unrated': 'x7', 'G': 'G1', 'PG': 'P2', \
               'PG-13': 'P3', 'R': 'R4', 'NC-17': 'N6', 'NR': 'N7' }
            
            if 'rating' in data:
                if ratings[ data['rating'] ][0] == 'x':
                    data['tvRating'] = ratings[ data['rating'] ]
                else:
                    data['mpaaRating'] = ratings[ data['rating'] ]

        except:
            pass
        
        if vdvd.FileTitle().HasAngles():
            if 'description' in data:    
                data['description'] = "[Angle] " + data['description']
            else:
                data['description'] = "[Angle]"
        elif vdvd.FileTitle().HasInterleaved():
            if 'description' in data:
                data['description'] = "[ILVU] " + data['description']
            else:
                data['description'] = "[ILVU]"
            
        data['seriesTitle'] = data['title']

        data['isEpisode'] = "true"
        data['episodeTitle'] = vdvd.TitleName()
        data['episodeNumber'] = str(vdvd.TitleNumber())
        data['seriesId'] = str(vdvd.TitleNumber())
        data['callsign'] = "DVD"
        data['displayMajorNumber'] = "1"
        data['displayMinorNumber'] = "0"

        if config.getDebug() and 'vHost' not in data:
            vobstream_options = vobstream.vobstream(True, full_path,
                                                        '', tsn)
            data['vHost'] = (
                ['TRANSCODE=%s, %s' % ( 'YES', 'All DVD Video must be re-encapsulated' )] +
                ['SOURCE INFO: '] +
                ["%s=%s" % (k, v)
                 for k, v in sorted(vInfo.items(), reverse=True)] +
                ['TRANSCODE OPTIONS: '] +
                ["%s" % (v) for k, v in vobstream_options.items()] +
                ['SOURCE FILE: ', os.path.split(full_path)[1]]
            )

        now = datetime.utcnow()
        duration = int( vInfo['millisecs'] )
        duration_delta = timedelta(milliseconds = duration)
        min = duration_delta.seconds / 60
        sec = duration_delta.seconds % 60
        hours = min / 60
        min = min % 60

        data.update({'time': now.isoformat(),
                     'startTime': now.isoformat(),
                     'stopTime': (now + duration_delta).isoformat(),
                     'size': self.__est_size(full_path, tsn, mime),
                     'duration': duration,
                     'iso_duration': ('P%sDT%sH%sM%sS' % 
                          (duration_delta.days, hours, min, sec))})

        return data

    def QueryContainer(self, handler, query):
        #print "************************************* In QueryContainer"
        tsn = handler.headers.getheader('tsn', '')
        subcname = query['Container'][0]
        cname = subcname.split('/')[0]
        dir_path = self.get_local_path(handler, query)
        dvd = None

        if (not cname in handler.server.containers or
            not self.get_local_path(handler, query)):
            handler.send_error(404)
            return

        container = handler.server.containers[cname]
        force_alpha = container.get('force_alpha', 'False').lower() == 'true'
        audio_spec = container.get('audio_spec', '')

        # Patch in our virtual filesystem if it exists
        while 1:
            if os.path.isdir(dir_path):
                dvd = virtualdvd.VirtualDVD( dir_path, \
                    float( container.get( 'title_min', '10.0' ) ) )
                
                # If the DVD folder is valid, then get the files and process them
                if dvd.Valid() or dvd.HasErrors():
                    files = dvd.GetFiles()
                    files, total, start = self.item_count( handler, query, cname, files, 0 )
                    break

            files, total, start = self.get_files(handler, query,
                                                 self.video_file_filter,
                                                 force_alpha)
            break

        videos = []
        local_base_path = self.get_local_base_path(handler, query)
        for f in files:
            mtime = datetime.fromtimestamp(f.mdate)
            video = VideoDetails()
            video['captureDate'] = hex(int(time.mktime(mtime.timetuple())))
            video['part_path'] = f.name.replace(local_base_path, '', 1)
            if not video['part_path'].startswith(os.path.sep):
                video['part_path'] = os.path.sep + video['part_path']
            video['title'] = os.path.split(f.name)[1]
            video['name'] = os.path.split(f.name)[1]
            video['path'] = f.name
        
            video['is_dir'] = f.isdir
            if video['is_dir']:
                video['small_path'] = subcname + '/' + video['name']
                
                sub_dvd = virtualdvd.VirtualDVD( f.name )
                if sub_dvd.QuickValid():
                    # Greatly speed up listing
                    if container.get('fast_listing', 'False').lower() == 'false':
                        video['total_items'] = sub_dvd.NumFiles()
                    else:
                        video['total_items'] = 1
                
                elif sub_dvd.HasErrors():
                    video['total_items'] = 0
                    
                else:
                    video['total_items'] = self.__total_items(f.name)
            else:
                if dvd != None and dvd.HasErrors():
                    video['title'] = "Error in DVD Format"
                    video['callsign'] = "DVD"
                    video['displayMajorNumber'] = "1"
                    video['displayMinorNumber'] = "0"
                    video['description'] = f.title
                    
                #if len(files) == 1 or f.name in vobstream.info_cache:
                elif f.name in vobstream.info_cache:
                    video['valid'] = vobstream.supported_format(f.name)
                    if video['valid']:
                        video.update(self.metadata_full(f.name, tsn, audio_spec=audio_spec))
                #elif dvd != None and ( dvd.Valid() or dvd.HasErrors() ):
                elif dvd != None and dvd.Valid():
                    video['valid'] = True
                    video['title'] = dvd.DVDTitleName()
                    video['seriesTitle'] = dvd.DVDTitleName()
                    video['isEpisode'] = "true"
                    video['episodeTitle'] = f.title
                    video['episodeNumber'] = str(dvd.TitleNumber())
                    video['callsign'] = "DVD"
                    video['displayMajorNumber'] = "1"
                    video['displayMinorNumber'] = "0"

                else:
                    video['valid'] = True
                    video.update(metadata.basic(f.name))

            videos.append(video)

        t = Template(CONTAINER_TEMPLATE, filter=EncodeUnicode)
        t.container = cname
        t.name = subcname
        t.total = total
        t.start = start
        t.videos = videos
        t.quote = quote
        t.escape = escape
        t.crc = zlib.crc32
        t.guid = config.getGUID()
        t.tivos = config.tivos
        t.tivo_names = config.tivo_names
                
        handler.send_response(200)
        handler.send_header('Content-Type', 'text/xml')
        handler.end_headers()
        handler.wfile.write(t)

        #print t

    def TVBusQuery(self, handler, query):
        #print "************************************* In TVBus"
        tsn = handler.headers.getheader('tsn', '')
        f = query['File'][0]
        subcname = query['Container'][0]
        cname = subcname.split('/')[0]
        container_obj = handler.server.containers[cname]
        audio_spec = container_obj.get('audio_spec', '')

        path = self.get_local_path(handler, query)
        file_path = path + os.path.normpath(f)

        file_info = VideoDetails()
        file_info['valid'] = vobstream.supported_format(file_path)
        if file_info['valid']:
            file_info.update(self.metadata_full(file_path, tsn, audio_spec=audio_spec))

        t = Template(TVBUS_TEMPLATE, filter=EncodeUnicode)
        t.video = file_info
        t.escape = escape
        handler.send_response(200)
        handler.send_header('Content-Type', 'text/xml')
        handler.end_headers()
        handler.wfile.write(t)

        #print t

    def XSL(self, handler, query):
        #print "************************************* In XSL"
        handler.send_response(200)
        handler.send_header('Content-Type', 'text/xml')
        handler.end_headers()
        handler.wfile.write(XSL_TEMPLATE)

    def Push(self, handler, query):
        #print "************************************* In Push"
        tsn = query['tsn'][0]
        for key in config.tivo_names:
            if config.tivo_names[key] == tsn:
                tsn = key
                break

        container = quote(query['Container'][0].split('/')[0])
        container_obj = handler.server.containers[container]
        audio_spec = container_obj.get('audio_spec', '')

        ip = config.get_ip()
        port = config.getPort()

        baseurl = 'http://%s:%s' % (ip, port)
        if config.getIsExternal(tsn):
            exturl = config.get_server('externalurl')
            if exturl:
                baseurl = exturl
            else:
                ip = self.readip()
                baseurl = 'http://%s:%s' % (ip, port)
 
        path = self.get_local_base_path(handler, query)

        for f in query.get('File', []):
            file_path = path + os.path.normpath(f)

            file_info = VideoDetails()
            file_info['valid'] = vobstream.supported_format(file_path)

            mime = 'video/mpeg'

            if file_info['valid']:
                file_info.update(self.metadata_full(file_path, tsn, mime, audio_spec=audio_spec))

            url = baseurl + '/%s%s' % (container, quote(f))

            title = file_info['seriesTitle']
            if not title:
                title = file_info['title']

            source = file_info['seriesId']
            if not source:
                source = title

            subtitle = file_info['episodeTitle']
            logger.debug('Pushing ' + url)
            try:
                m = mind.getMind(tsn)
                m.pushVideo(
                    tsn = tsn,
                    url = url,
                    description = file_info['description'],
                    duration = file_info['duration'] / 1000,
                    size = file_info['size'],
                    title = title,
                    subtitle = subtitle,
                    source = source,
                    mime = mime)
            except Exception, e:
                handler.send_response(500)
                handler.end_headers()
                handler.wfile.write('%s\n\n%s' % (e, traceback.format_exc() ))
                raise

        referer = handler.headers.getheader('Referer')
        handler.send_response(302)
        handler.send_header('Location', referer)
        handler.end_headers()

    def readip(self):
        """ returns your external IP address by querying dyndns.org """
        f = urllib.urlopen('http://checkip.dyndns.org/')
        s = f.read()
        m = re.search('([\d]*\.[\d]*\.[\d]*\.[\d]*)', s)
        return m.group(0)

class VideoDetails(DictMixin):

    def __init__(self, d=None):
        if d:
            self.d = d
        else:
            self.d = {}

    def __getitem__(self, key):
        if key not in self.d:
            self.d[key] = self.default(key)
        return self.d[key]

    def __contains__(self, key):
        return True

    def __setitem__(self, key, value):
        self.d[key] = value

    def __delitem__(self):
        del self.d[key]

    def keys(self):
        return self.d.keys()

    def __iter__(self):
        return self.d.__iter__()

    def iteritems(self):
        return self.d.iteritems()

    def default(self, key):
        defaults = {
            'showingBits' : '0',
            'episodeNumber' : '0',
            'displayMajorNumber' : '0',
            'displayMinorNumber' : '0',
            'isEpisode' : 'false',
            'colorCode' : ('COLOR', '4'),
            'showType' : ('SERIES', '5'),
            'tvRating' : ('NR', '7')
        }
        if key in defaults:
            return defaults[key]
        elif key.startswith('v'):
            return []
        else:
            return ''
