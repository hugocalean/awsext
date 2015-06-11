# Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Extensions to boto S3Connection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import os
import shutil
import threading
import boto.s3.connection


class AwsExtS3Connection(boto.s3.connection.S3Connection):
    """ """

    def __init__(self, **kw_params):
        """

        :param **kw_params: 

        """
        super(AwsExtS3Connection, self).__init__(**kw_params)

    def sync_from_s3(self, bucket_name=None, prefix=None, local_path=None, clean_first=True ):
        """Transfer all files from and s3 bucket and prefix to a local path

        :param bucket_name: source S3 bucket (Default value = None)
        :param prefix: path in S3 bucket to filter objects (Default value = None)
        :param local_path: target local path where S3 bucket/prefix files will be transferre (Default value = None)
        :param clean_first: delete all files in target local path (Default value = True)

        """
        # Clean up and create staging directory
        if clean_first and os.path.exists(local_path): shutil.rmtree(local_path)
        os.makedirs(local_path)
        # Get list of all keys in the bucket
        bucket = self.get_bucket( bucket_name )
        keys = bucket.list( prefix=prefix+'/')
        download_from_s3_threads = []
        for key in keys: download_from_s3_threads.append( DownloadFromS3Thread( key, local_path))
        # TODO: thread pool instead of one thread per transfer
        for download_from_s3_thread in download_from_s3_threads: download_from_s3_thread.start()
        for download_from_s3_thread in download_from_s3_threads: download_from_s3_thread.join()
        for download_from_s3_thread in download_from_s3_threads: 
            if download_from_s3_thread.error != None: raise download_from_s3_thread.error



class DownloadFromS3Thread(threading.Thread):
    """Support concurrent downloads from S3, each on it's own thread """
                                
    def __init__( self, key, local_path ):
        """

        :param key: source S3 object key
        :param local_path: target local path

        """
        threading.Thread.__init__(self)
        self.key = key
        self.local_path = local_path
        self.error = None
        
    
    def run(self):
        """ """
        try:
            last_slash_pos = self.key.name.rfind('/')
            if last_slash_pos != -1: key_path = self.key.name[0:last_slash_pos]
            else: key_path = None
            if key_path != None:
                local_full_path = self.local_path + '/' + key_path
                if not os.path.exists( local_full_path ):
                    try: 
                        os.makedirs( local_full_path )
                    except StandardError as e:
                        if not e.strerror == 'File exists': raise e
            self.key.get_contents_to_filename(self.local_path + '/' + self.key.name)
        except StandardError as e:
            self.error = e

        
        