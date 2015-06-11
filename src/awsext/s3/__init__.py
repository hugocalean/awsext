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
Common methods for awsext.s3
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

from boto.regioninfo import RegionInfo, get_regions
import boto.s3

def regions():
    """Get all available regions for the Amazon S3 service.
    
    :return: A list of configured ``RegionInfo`` objects
    
    """
    from awsext.s3.connection import AwsExtS3Connection
    return get_regions(
        's3',
        region_cls=boto.s3.S3RegionInfo,
        connection_cls=AwsExtS3Connection
    )


def connect_to_region(region_name, **kw_params):
    """Create AwsExtS3Connection in specified region

    :param region_name: param **kw_params:
    :param **kw_params: 
    :return: instance of awsext.s3.connectino.AwsExtS3Connection

    """
    for region in regions():
        if 'host' in kw_params.keys():
            # Make sure the host specified is not nothing
            if kw_params['host'] not in ['', None]:
                region.endpoint = kw_params['host']
                del kw_params['host']
                return region.connect(**kw_params)
            # If it is nothing then remove it from kw_params and proceed with default
            else:
                del kw_params['host']
        if region.name == region_name:
            return region.connect(**kw_params)
    return None
