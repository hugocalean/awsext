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
Common methods for awsext.sqs
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import boto.sqs
from boto.regioninfo import get_regions
from boto.sqs.regioninfo import SQSRegionInfo


def regions():
    """Get all available regions for the SQS service.
    
    :return: A list of configured ``RegionInfo`` objects
    
    """
    return get_regions(
        'sqs',
        region_cls=IpcSQSRegionInfo
    )
    
    
def connect_to_region( region_name, **kw_params):
    """Create AwsExtSQSConnection in specified region

    :param region_name: param **kw_params:
    :param **kw_params: 
    :return: Instance of awsext.sqs.connection.AwsExtSQSConnection

    """

    for region in regions():
        if region.name == region_name:
            return region.connect(**kw_params)
    return None


class IpcSQSRegionInfo(SQSRegionInfo):
    """Contains attributes describing SQS connection in a given region """

    def __init__(self, connection=None, name=None, endpoint=None,
                 connection_cls=None):
        """

        :param connection: instance of AwsExtSQSConnection (Default value = None)
        :param name: queue name (Default value = None)
        :param endpoint: queue endpoint, including region name (Default value = None)
        :param connection_cls: awsext.sqs.connection.AwsExtSQSConnection (Default value = None)

        """
        from awsext.sqs.connection import AwsExtSQSConnection
        super(IpcSQSRegionInfo, self).__init__(connection, name, endpoint,
                            AwsExtSQSConnection)
        self.connection_cls = AwsExtSQSConnection
