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
Common methods for awsext.iam
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

from boto.regioninfo import RegionInfo, get_regions
from boto.iam import IAMRegionInfo
from awsext.iam.connection import AwsExtIAMConnection


def regions():
    """Get all available regions for the IAM service."""
    regions = get_regions(
        'iam',
        region_cls=IAMRegionInfo,
        connection_cls=AwsExtIAMConnection
    )

    # For historical reasons, we had a "universal" endpoint as well.
    regions.append(
        IAMRegionInfo(
            name='universal',
            endpoint='iam.amazonaws.com',
            connection_cls=AwsExtIAMConnection
        )
    )

    return regions


def connect_to_region(region_name, **kw_params):
    """Given a valid region name, return a
    :class:`awsext.iam.connection.AwsExtIAMConnection`.

    :param region_name: The name of the region to connect to.
    :param **kw_params: 
    :return: :class:`awsext.iam.connection.AwsExtIAMConnection`

    """
    for region in regions():
        if region.name == region_name:
            return region.connect(**kw_params)
    return None


def connect_iam(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """Create AwsExtIAMConnection 

    :param aws_access_key_id: Your AWS Access Key ID (Default value = None)
    :param aws_secret_access_key: Your AWS Secret Access Key (Default value = None)
    :param **kwargs:
    :return: :class:`awsext.iam.connection.AwsExtIAMConnection`

    """
    from awsext.iam.connection import AwsExtIAMConnection
    return AwsExtIAMConnection(aws_access_key_id, aws_secret_access_key, **kwargs)
