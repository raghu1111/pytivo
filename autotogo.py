import cookielib
import logging
import os
import sys
import time
import re
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

class AutoToGo:

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
        togo.auth_handler.add_password('TiVo DVR', tivoIP, 'tivo', tivo_mak)
        myToGo = ToGo()
        page = myToGo.tivo_open(theurl)
        thepage = minidom.parse(page)
        page.close()
        items = thepage.getElementsByTagName('Item')

        saved_ids = myToGo.get_saved_ids()

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
                    capture_time = int(tag_data(item, 'Details/CaptureDate'), 16)
                    #don't auto_get programs older than 3days (should Title specific)
                    if (time.time() - capture_time > 3*86400):
                        continue

                    #check if id is already downloaded
                    if re.search('id=(\d+)', theUrl).group(1) in saved_ids:
                        continue

                    if not (tivoIP in togo.queue and theUrl in togo.queue[tivoIP]):
                        logger.info('adding %s' % theUrl)
                        togo.basic_meta[theUrl] = metadata.from_container(item)
                        myToGo.enqueue_url(theUrl, tivoIP, tivo_mak, togo_path, True, True)

    def start(self):
        # Only activate if we have titles defined to transfer
        if config.auto_togo():
            self.transfer()
            self.timer = Timer(1800, self.start)
            self.timer.start()

    def stop(self):
        if hasattr(self, 'timer'):
            self.timer.cancel()
