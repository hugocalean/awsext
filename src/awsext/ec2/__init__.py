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
Common methods for awsext.ec2
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

INSTANCE_STATE_CODE_PENDING = 0
INSTANCE_STATE_CODE_RUNNING = 16
INSTANCE_STATE_CODE_SHUTTINGDOWN = 32
INSTANCE_STATE_CODE_TERMINATED = 48
INSTANCE_STATE_CODE_STOPPING = 64
INSTANCE_STATE_CODE_STOPPED = 80

from boto.regioninfo import get_regions, RegionInfo
import awsext.ec2.connection


def regions(**kw_params):
    """Get all available regions for the EC2 service.
    You may pass any of the arguments accepted by the EC2Connection
    object's constructor as keyword arguments and they will be
    passed along to the EC2Connection object.

    :param **kw_params: 

    """
    return get_regions('ec2', connection_cls=awsext.ec2.connection.AwsExtEC2Connection)



def connect_to_region(region_name, **kw_params):
    """Given a valid region name, return a
    :class:`awsext.ec2.connection.AwsExtEC2Connection`.
    Any additional parameters after the region_name are passed on to
    the connect method of the region object.

    :param region_name: The name of the region to connect to.
    :param **kw_params: 

    """
    if 'region' in kw_params and isinstance(kw_params['region'], RegionInfo)\
       and region_name == kw_params['region'].name:
        return awsext.ec2.connection.AwsExtEC2Connection(**kw_params)

    for region in regions(**kw_params):
        if region.name == region_name:
            return region.connect(**kw_params)

    return None