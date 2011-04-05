import cookielib
import logging
import os
import sys
import time
import urllib2
import urlparse
from urllib import quote, unquote
from xml.dom import minidom
from threading import Timer
import thread
from plugins.togo import togo
from plugins.togo.togo import ToGo

import config
import metadata

logger = logging.getLogger('pyTivo.autotogo')
tag_data = metadata.tag_data

auth_handler = urllib2.HTTPDigestAuthHandler()
cj = cookielib.LWPCookieJar()
tivo_opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj),
                                   auth_handler)

class AutoToGo:
    def tivo_open(self, url):
        # Loop just in case we get a server busy message
        while True:
            try:
                # Open the URL using our authentication/cookie opener
                return tivo_opener.open(url)

            # Do a retry if the TiVo responds that the server is busy
            except urllib2.HTTPError, e:
                if e.code == 503:
                    time.sleep(5)
                    continue

                # Throw the error otherwise
                raise

    def transfer(self):
        for tivo in config.tivos:
            tivoIP = config.tivos[tivo]
            tivo_name = config.tivo_names[tivo]
            theurl = ('https://' + tivoIP +
                      '/TiVoConnect?Command=QueryContainer' +
                      '&Container=/NowPlaying')
            self.get_npl(tivoIP, theurl)

    def get_npl(self, tivoIP, theurl):
        tivo_mak = config.get_server('tivo_mak')
        togo_path = config.get_server('togo_path')
        auth_handler.add_password('TiVo DVR', tivoIP, 'tivo', tivo_mak)
        page = self.tivo_open(theurl)
        thepage = minidom.parse(page)
        page.close()
        items = thepage.getElementsByTagName('Item')
        for item in items:
            if tag_data(item, 'Title') in config.auto_togo():
                # Skip in progress recordings
                if tag_data(item, 'Links/CustomIcon/Url') == 'urn:tivo:image:in-progress-recording':
                    logger.info('skipping in progress recording')
                    continue
                # Handle folders Recursively
                if tag_data(item, 'ContentType') == 'x-tivo-container/folder':
                    self.get_npl(tivoIP,
                                 "%s/%s" % (theurl, 
                                            tag_data(item, 'UniqueId')))
                else:
                    theUrl = tag_data(item, 'Url')
    
                    #Figure out the filename so we can 
                    # see if we already have it
                    parse_url = urlparse.urlparse(theUrl)
                    name = unquote(parse_url[2])[10:].split('.')
                    id = unquote(parse_url[4]).split('id=')[1]
                    name.insert(-1, ' - ' + id + '.')
                    name[-1] = 'mpg'
                    outfile = os.path.join(togo_path,
                                           ''.join(name))
    
                   # Would like to support Directories for outfile
                                          #tag_data(item, 'Title'),
    
                    if not os.path.exists(outfile) and not (
                       tivoIP in togo.queue and \
                       theUrl in togo.queue[tivoIP]):
                        logger.info('adding %s' % theUrl)
                        if not os.path.exists(os.path.dirname(outfile)):
                            os.mkdir(os.path.dirname(outfile))
                        togo.status[theUrl] = {'running': False, 'error': '', 
                                    'rate': '', 'queued': True, 
                                    'size': 0, 'finished': False,
                                    'decode': bool(config.get_bin('tivodecode')),
                                    'save': True}
                        togo.basic_meta[theUrl] = metadata.from_container(item)
                        if tivoIP not in togo.queue:
                            togo.queue[tivoIP] = [theUrl]
                            myToGo = ToGo()
                            thread.start_new_thread(myToGo.process_queue,
                                          (tivoIP, tivo_mak, togo_path))
                        else:
                            togo.queue[tivoIP].append(theUrl)

    def start(self):
        # Only activate if we have titles defined to transfer
        if config.auto_togo():
            self.transfer()
            self.timer = Timer(60, self.start)
            self.timer.start()

    def stop(self):
        if hasattr(self, 'timer'):
            self.timer.cancel()
