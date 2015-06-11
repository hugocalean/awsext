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
Extensions to boto VPCConnection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import awsext.exception
import boto.vpc

import logging
logger = logging.getLogger(__name__)


class AwsExtVPCConnection(boto.vpc.VPCConnection):
    """ """

    def __init__(self, **kw_params):
        """

        :param **kw_params: 

        """
        super(AwsExtVPCConnection, self).__init__(**kw_params)
    
    
    def delete_security_group_sync( self, vpc_id, group_name=None, group_id=None, poll_interval_secs=5, poll_max_minutes=10 ):
        """Delete security group and wait for completion

        :param vpc_id: 
        :param param group_name:  (Default value = None)
        :param group_id: Default value = None)
        :param poll_interval_secs: Default value = 5)
        :param poll_max_minutes: Default value = 10)
        :return: True if delete/wait for completion is successful
        :raise awsext.exception.SecurityGroupDoesntExistError: security group doesn't exist

        """
        group_id, is_group_exists = self.is_security_group_exists( vpc_id, group_name=group_name, group_id=group_id)
        if not is_group_exists:
            if group_name != None: error_parm = group_name
            else: error_parm = group_id 
            raise awsext.exception.SecurityGroupDoesntExistError( error_parm, error_parm)
        self.delete_security_group( group_id=group_id )
        self.poll_security_group( vpc_id, group_id, target_group_exists=False, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        return True
                    
    
    def create_security_group_sync( self, vpc_id, sg_name, sg_group_name, sg_desc,
                               poll_interval_secs=5, poll_max_minutes=10, inbound_rule_items=None ):
        """Create security group and wait for completion

        :param vpc_id: vpc id
        :param sg_name: value to use for Name tag
        :param sg_group_name: security group name
        :param sg_desc: security group description
        :param poll_interval_secs: polling interval (Default value = 5)
        :param poll_max_minutes: max polling minutes before timeout (Default value = 10)
        :param inbound_rule_items: (Default value = None)
        :return: :class:`boto.ec2.securitygroup.SecurityGroup`
        :raise awsext.exception.SecurityGroupTimeoutError: timeout waiting for completion
        
        """
        
        group_id, is_group_exists = self.is_security_group_exists( vpc_id, group_name=sg_group_name )
        if is_group_exists: raise awsext.exception.SecurityGroupAlreadyExistsError( sg_group_name, sg_group_name)
        security_group = self.create_security_group( sg_group_name, sg_desc, vpc_id=vpc_id )    
        self.poll_security_group( vpc_id, security_group.id, target_group_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        security_group.add_tag( 'Name', value=sg_name )
        if inbound_rule_items != None: self.authorize_inbound_rules( security_group, inbound_rule_items )
        return security_group
            
    
    def create_unique_security_group( self, vpc_id, sg_name_prefix, inbound_rule_items=None,
                                      max_attempts=100 ):
        """

        :param vpc_id: vpc id
        :param sg_name_prefix: prefix of unique sg name
        :param inbound_rule_items: list of ingress rules as :class:awsext.vpc.connection.InboundRuleItem (Default value = None)
        :param max_attempts: max attempts before exception (Default value = 100)
        :return: :class:`boto.ec2.securitygroup.SecurityGroup`

        """
        return self.create_unique_security_group_sync( vpc_id, sg_name_prefix, inbound_rule_items=None,
                                      max_attempts=100, is_sync=False )  
            
    
    def create_unique_security_group_sync( self, vpc_id, sg_name_prefix, inbound_rule_items=None,
                                      max_attempts=100, poll_interval_secs=5, poll_max_minutes=10, is_sync=True):    
        """

        :param vpc_id: vpc id
        :param sg_name_prefix: prefix of unique sg name
        :param inbound_rule_items: list of ingress rules as :class:awsext.vpc.connection.InboundRuleItem (Default value = None)
        :param max_attempts: max attempts before awsext.exception.SecurityGroupAlreadyExistsError (Default value = 100)
        :param poll_interval_secs: polling interval (Default value = 5)
        :param poll_max_minutes: max minutes to poll before timeout (Default value = 10)
        :param is_sync: if True, then poll, else return immediately (Default value = True)
        :return: :class:`boto.ec2.securitygroup.SecurityGroup`
        :raise awsext.exception.SecurityGroupAlreadyExistsError: tried from 0 to max_attempts and all SG's already exist 

        """
        # Create uniquely named SG
        security_group = None
        start_suffix = int(time.time()*100)
        for unique_suffix in range( start_suffix, (max_attempts+start_suffix) ):
            sg_name_desc = sg_name_prefix + str( unique_suffix )
            group_id, is_group_exists =  self.is_security_group_exists( vpc_id, group_name=sg_name_desc )
            if is_group_exists: continue
            security_group = self.create_security_group( sg_name_desc, sg_name_desc, vpc_id=vpc_id )
            break;
        if security_group == None: raise awsext.exception.SecurityGroupAlreadyExistsError( sg_name_desc, sg_name_desc)
        if is_sync:
            self.poll_security_group( vpc_id, security_group.id, target_group_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        if inbound_rule_items != None and is_sync: self.authorize_inbound_rules( security_group, inbound_rule_items )
        return security_group
    
    
    def authorize_inbound_rules( self, security_group, inbound_rule_items ):
        """Apply ingress rules based on list of InboundRuleItem instances

        :param security_group: SecurityGroup instance
        :param inbound_rule_items: list of InboundRuleItem instances 

        """
        for inbound_rule_item in inbound_rule_items:
            security_group.authorize( ip_protocol=inbound_rule_item.ip_protocol, from_port=inbound_rule_item.from_port, 
                                      to_port=inbound_rule_item.to_port, cidr_ip=inbound_rule_item.cidr_ip )   
                

    def is_security_group_exists( self, vpc_id, group_name=None, group_id=None):
        """Check if security group exists

        :param vpc_id: 
        :param group_name: security group name (Default value = None)
        :param group_id: security group id (Default value = None)

        """
        filters = { 'vpc_id': vpc_id,}
        is_group_exists = False
        groups = self.get_all_security_groups( filters=filters )
        for group in groups: 
            if (group_name != None and group_name == group.name) or (group_id != None and group_id == group.id):
                group_id = group.id
                is_group_exists = True
                break;
        return group_id, is_group_exists

  
    def poll_security_group( self, vpc_id, group_id, target_group_exists=True,
                                    poll_interval_secs=5, poll_max_minutes=10 ):
        """Poll for exists/not exists based on value of target_group_exists

        :param vpc_id: vpc id
        :param group_id: group id
        :param target_group_exists: if True, then SG must exist to return, else if False the SG must not exist to return. (Default value = True)
        :param poll_interval_secs: polling interval in seconds (Default value = 5)
        :param poll_max_minutes: max minutes to poll before timeout (Default value = 10)
        :return: True if target_group_exists matches the current state of the security group
        :raise awsext.exception.SecurityGroupTimeoutError: target_group_exists didn't match within poll_max_minutes

        """
        # Its possible that the delete_security_group returned but the SG isn't deleted yet - poll for it
        filters = { 'vpc_id': vpc_id,}
        expires_at = time.time() + (poll_max_minutes * 60)
        while time.time() <= expires_at:
            is_group_exists = False
            groups = self.get_all_security_groups( filters=filters )
            for group in groups:
                if group_id == group.id:
                    is_group_exists = True
                    break
            if target_group_exists == is_group_exists: return
            time.sleep( poll_interval_secs )
    
        raise awsext.exception.SecurityGroupTimeoutError( group_id )

  
    def get_security_group( self, vpc_id, group_id ):
        """Find and return SecurityGroup instance

        :param vpc_id: vpc id
        :param group_id: security group id
        :return: instance of SecurityGroup or None if not found

        """
        filters = { 'vpc_id': vpc_id }
        groups = self.get_all_security_groups( filters=filters )
        for group in groups:
            if group_id == group.id: return group
        return None


class InboundRuleItem:
    """Attributes necessary to define an ingress rule """
    def __init__(self, ip_protocol='tcp', from_port=22, to_port=None, cidr_ip='0.0.0.0/0' ):
        """

        :param ip_protocol: ip protocol (Default value = 'tcp')
        :param from_port: from port (Default value = 22)
        :param to_port: to port (Default value = None)
        :param cidr_ip: CIDR (Default value = '0.0.0.0/0')

        """
        self.ip_protocol = ip_protocol
        self.from_port = from_port
        if to_port != None: self.to_port = to_port
        else: self.to_port = from_port
        self.cidr_ip = cidr_ip
