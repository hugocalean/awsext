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
Spot Instance Pricing classes and methods
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import operator
import boto.ec2
import boto.vpc
import awsext.ec2.connection
from awsext.vpc.connection import InboundRuleItem

import logging
logger = logging.getLogger(__name__)


def find_spot_cheapest_prices( instance_type='m3.large', product_description='Linux/UNIX', profile_name=None, region_filter=None, max_bid=None, verbose=False):
    """Find the cheapest spot price based on various criteria

    :param instance_type: EC2 instance type (Default value = 'm3.large')
    :param product_description: Linux/UNIX or Windows (Default value = 'Linux/UNIX')
    :param profile_name: profile name from credentials file (Default value = None)
    :param region_filter: list of regions to be checked(Default value = None)
    :param max_bid: only create SpotCheapestItem instance if Region/AZ spot price is <= max_bid (Default value = None)
    :param verbose: If True, log detailed status messages. (Default value = False)
    :return: list of :class:awsext.ec2.spotprice.SpotCheapestItem 

    """
    spot_cheapest_items = []
    
    all_regions = boto.ec2.regions( profile_name=profile_name )
    for region in all_regions:
        if region_filter != None and not region.name in region_filter: continue
        try:
            ec2_conn_region = boto.ec2.connect_to_region( region.name, profile_name=profile_name )
            zones = ec2_conn_region.get_all_zones()
        except boto.exception.EC2ResponseError as e:
            if e.code == 'AuthFailure':
                if verbose: logger.warn( 'Not authorized for region: ' + region.name )
                continue
            else: raise e            
        for zone in zones:
            if verbose: logger.info( 'Checking Zone: ' + zone.name )
            spot_price_histories = ec2_conn_region.get_spot_price_history( instance_type=instance_type, product_description=product_description, max_results=1, availability_zone=zone.name )
            if len(spot_price_histories) > 0:
                if max_bid == None or spot_price_histories[0].price <= max_bid:
                    spot_cheapest_items.append( SpotCheapestItem( instance_type, product_description, region, zone, spot_price_histories[0].price ) )

    spot_cheapest_items.sort( key=operator.attrgetter('price'))
    return spot_cheapest_items


class SpotCheapestItem():
    """Contains all attributes to describe a cheapest spot price """

    def __init__(self, instance_type, product_description, region, zone, price ):           
        """

        :param instance_type: EC2 instance type
        :param product_description: Linux/UNIX or Windows
        :param region: Region
        :param zone: Availability Zone
        :param price: Spot Price 

        """
        self.instance_type = instance_type
        self.product_description = product_description
        self.region = region
        self.zone = zone
        self.price = price
    
    
    def is_valid( self ):
        """ """
        if self.region != None: return True
        else: return False
        
        
    def __str__(self):
        """ """
        region_name = 'None Found'
        zone_name = 'None Found'
        price = 'N/A'
        if self.region != None: 
            region_name = self.region.name
            zone_name = self.zone.name
            price = str(self.price)
        return 'SpotCheapestItem: instance_type=' + self.instance_type + ', product_description=' + self.product_description + ', region.name=' + region_name + ', zone.name=' + zone_name + ', price=' + price


class SpotRegionItem():
    """Contains/creates attributes necessary to request Spot EC2 instances in a given region """
    
    def __init__(self, region_name, profile_name=None, kp_name_prefix='kp_spot_', key_path=None, vpc_id=None, sg_name_prefix='sg_spot_' ): 
        """

        :param region_name: Region name
        :param profile_name: Profile name from credentials file (Default value = None)
        :param kp_name_prefix: KeyPair prefix, used to create unique KeyPair   (Default value = 'kp_spot_')
        :param key_path: path/name.ext to store KeyPair .pem file (Default value = None)
        :param vpc_id: VPC to create spot instances in (Default value = None)
        :param sg_name_prefix: SecurityGroup prefix, used to create unique SecurityGroup (Default value = 'sg_spot_')

        """
        if key_path == None: raise ValueError('key_path is required')
        if vpc_id == None: raise ValueError('vpc_id is required')
        
        self.vpc_conn = boto.vpc.connect_to_region( region_name, profile_name=profile_name )
        self.ec2_conn = boto.ec2.connect_to_region( region_name, profile_name=profile_name )
    
        key_pair = awsext.ec2.connection.AwsExtEC2Connection.create_unique_key_pair_sync( kp_name_prefix, key_path )
        self.key_name = key_pair.name
    
        self.security_group = self.vpc_conn.create_unique_security_group( vpc_id, sg_name_prefix, 
                                                            inbound_rule_items= [ InboundRuleItem( from_port=22 )]
                                                            )
        self.security_group_ids = [ self.security_group.id ]

