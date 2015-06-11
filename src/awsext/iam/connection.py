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
Extensions to boto IAMConnection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import boto.iam.connection
import awsext.exception

import logging
logger = logging.getLogger(__name__)


class AwsExtIAMConnection(boto.iam.connection.IAMConnection):
    """ """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
        """

        :param aws_access_key_id:  (Default value = None)
        :param aws_secret_access_key:  (Default value = None)
        :param **kwargs: 

        """
        super(AwsExtIAMConnection, self).__init__(aws_access_key_id=None, aws_secret_access_key=None, **kwargs)


    def create_role_instance_profile_sync( self, policy=None, role_name=None, policy_name=None, 
                                           poll_interval_secs=5, poll_max_minutes=10 ):
        """Create role, wait for completion, create instance profile, wait for completion, add role to instance profile, wait for completion

        :param policy: policy document (set of permissions) (Default value = None)
        :param role_name: role name (Default value = None)
        :param policy_name: policy name (Default value = None)
        :param poll_interval_secs: sleep interval(seconds) between polling attempts (Default value = 5)
        :param poll_max_minutes: (Default value = 10)
        :return: instance of awsext.iam.connection.RoleInstanceProfileItem
        :raise ValueError: missing required input values 

        """
        if policy == None: raise ValueError('Policy is required')
        if role_name == None: raise ValueError('Role Name is required')
        if policy_name == None: raise ValueError('Policy Name is required')

        self.create_instance_profile( role_name )
        self.poll_instance_profile_exists( role_name, target_instance_profile_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )
        self.create_role( role_name )
        self.poll_role_exists( role_name, target_role_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )
        self.add_role_to_instance_profile( role_name, role_name ) 
        self.put_role_policy( role_name, policy_name, policy)
        return RoleInstanceProfileItem( role_name, policy_name )


    def create_unique_role_instance_profile( self, policy=None, 
                                      role_name_prefix='role_', 
                                      policy_name_prefix='Policy-', max_attempts=100 ):
        """Create unique 

        :param policy: policy document (set of permissions) (Default value = None)
        :param role_name_prefix: prefix for unique role name (Default value = 'role_')
        :param policy_name_prefix: prefix for unique policy name (Default value = 'Policy-')
        :param max_attempts: max number of attempts to create unique role name or policy name (Default value = 100)
        :raise BotoServerError: if max attempts exceeded 

        """
        return self.create_unique_role_instance_profile_sync( policy=policy, 
                                                              role_name_prefix=role_name_prefix, 
                                                              policy_name_prefix=policy_name_prefix, 
                                                              max_attempts=100, 
                                                              is_sync=False )


    def create_unique_role_instance_profile_sync( self, policy=None, 
                                      role_name_prefix='role_', 
                                      policy_name_prefix='Policy-', max_attempts=100, 
                                      poll_interval_secs=5, poll_max_minutes=10, is_sync=True ):
        """

        :param policy: policy document (set of permissions) (Default value = None)
        :param role_name_prefix: prefix for unique role name (Default value = 'role_')
        :param policy_name_prefix: prefix for unique policy name (Default value = 'Policy-')
        :param max_attempts: max number of attempts to create unique role name or policy name (Default value = 100)
        :param poll_interval_secs: sleep interval(seconds) between polling attempts (Default value = 5)
        :param poll_max_minutes: max poll minutes before timeout (Default value = 10)
        :param is_sync: if True, poll for completion, else return immediately. (Default value = True)
        :return: instance of :class:awsext.iam.connection.RoleInstanceProfileItem
        :raise BotoServerError: if max attempts exceeded 

        """
        if policy == None: raise ValueError('Policy is required')
        policy_name = None
        role_name = None

        
        # Create unique instance profile name and verify it exists
        unique_suffix = int(time.time()*100)
        for i in range(1,(max_attempts+1)):
            role_name = role_name_prefix + str( unique_suffix )
            try:
                self.create_instance_profile( role_name )
                break;
            except boto.exception.BotoServerError as e:
                if e.error_code == 'EntityAlreadyExists' and i<max_attempts: unique_suffix += 1
                else: raise e
        
        # Its possible that the create_instance_profile returned but the IP isn't available yet - poll for it
        if is_sync: 
            self.poll_instance_profile_exists( role_name, target_instance_profile_exists=True )
    
        # Create unique role name and verify it exists
        # CRITICAL: Role Name and Instance Profile name must be the same
        role_name = role_name_prefix + str( unique_suffix )
        try:
            self.create_role( role_name )
            policy_name = policy_name_prefix + str( unique_suffix )
        except boto.exception.BotoServerError as e:
            logger.error( e.get_message() )
            raise e
        
        # Its possible that the create_role returned but the role isn't available yet - poll for it
        if is_sync: 
            self.poll_role_exists( role_name, target_role_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )
        
        if is_sync:
            self.add_role_instance_profile_policy( role_name=role_name, policy_name=policy_name, policy=policy )
        
        return RoleInstanceProfileItem( role_name, policy_name )
    
    
    def add_role_instance_profile_policy(self, role_name=None, policy_name=None, policy=None ):
        """Add role to instance profile, add policy (name and document) to role 

        :param role_name: role name (Default value = None)
        :param policy_name: policy name (Default value = None)
        :param policy: policy document (Default value = None)

        """
        self.add_role_to_instance_profile( role_name, role_name ) 
        self.put_role_policy( role_name, policy_name, policy)
        
        
    def is_role_policy_added(self, role_name=None, policy_name=None):
        """Check if policy add to role has completed

        :param role_name: (Default value = None)
        :param policy_name: (Default value = None)
        :return: True if completed, False if not completed

        """
        list_role_policies_response = self.list_role_policies( role_name )
        if list_role_policies_response.list_role_policies_result.policy_names == None or len(list_role_policies_response.list_role_policies_result.policy_names) == 0: return False
        for check_policy_name in list_role_policies_response.list_role_policies_result.policy_names:
            if check_policy_name == policy_name: return True
        return False
    
    
    def delete_role_instance_profile_sync( self, role_instance_profile_item=None, role_name=None, policy_name=None, 
                                      poll_interval_secs=5, poll_max_minutes=10 ):
        """

        :param role_instance_profile_item: instance of :class:awsext.iam.connection.RoleInstanceProfileItem (Default value = None)
        :param role_name_prefix: prefix for unique role name (Default value = 'role_')
        :param policy_name_prefix: prefix for unique policy name (Default value = 'Policy-')
        :param poll_interval_secs: sleep interval(seconds) between polling attempts (Default value = 5)
        :param poll_max_minutes: max poll minutes before timeout (Default value = 10)

        """
        if role_instance_profile_item != None:
            role_name = role_instance_profile_item.role_name
            policy_name = role_instance_profile_item.policy_name
            
        if role_name == None: raise ValueError('Role Name is required')
        if policy_name == None: raise ValueError('Policy Name is required')

        self.remove_role_from_instance_profile( role_name)
        self.delete_role_policy( role_name, policy_name)  
        self.delete_role( role_name )
        self.poll_role_exists( role_name, target_role_exists=False )
        self.delete_instance_profile( role_name )
        self.poll_instance_profile_exists( role_name, target_instance_profile_exists=False )


    def poll_role_exists(self, role_name, target_role_exists=True, poll_interval_secs=5, poll_max_minutes=10 ):
        """Poll for role existence

        :param role_name: role name
        :param target_role_exists: If True, check if role_name exists, else check if role_name not exists (Default value = True)
        :param poll_interval_secs: sleep interval(seconds) between polling attempts (Default value = 5)
        :param poll_max_minutes: max poll minutes before timeout (Default value = 10)
        :return: True if target_role_exists and role exists, True if not target_role_exists and not role exists
        :raise awsext.exception.RoleTimeoutError: target_role_exists not matched within poll_max_minutes

        """
        expires_at = time.time() + (poll_max_minutes * 60)
        while time.time() <= expires_at:
            if target_role_exists == self.is_role_exists( role_name ): return
            time.sleep( poll_interval_secs )
        raise awsext.exception.RoleTimeoutError( role_name + ', target_role_exists=' + str(target_role_exists) )


    def is_role_exists(self, role_name, path_prefix=None):
        """Check if role exists

        :param role_name: role name
        :param path_prefix: role name (Default value = None)
        :return: True if role_name exists, else False

        """
        marker = None
        while True: 
            list_roles_response = self.list_roles( path_prefix=path_prefix, marker=marker )
            for role in list_roles_response.list_roles_result.roles:
                if role_name == role.role_name: return True
            if list_roles_response.list_roles_result.is_truncated == 'true': marker=list_roles_response.list_roles_result.marker
            else: break
        return False


    def poll_instance_profile_exists(self, role_name, target_instance_profile_exists=True, poll_interval_secs=5, poll_max_minutes=10 ):
        """Poll for instance profile existence

        :param role_name: role name
        :param poll_interval_secs: polling interval in seconds (Default value = 5)
        :param poll_max_minutes: max minutes before timeout (Default value = 10)
        :param target_instance_profile_exists: If True, check if role_name exists, else check if role_name not exists (Default value = True)
        :raise awsext.exception.InstanceTimeoutError: poll_max_minutes exceeded

        """
        expires_at = time.time() + (poll_max_minutes * 60)
        while time.time() <= expires_at:
            if target_instance_profile_exists == self.is_instance_profile_exists( role_name ): return
            time.sleep( poll_interval_secs )
        raise awsext.exception.InstanceTimeoutError( role_name + ', target_instance_profile_exists=' + str(target_instance_profile_exists) )
    

    def is_instance_profile_exists( self, role_name, path_prefix=None ):
        """Check if instance profile exists, assumes role name and instance profile name are exactly the same

        :param role_name: role name
        :param path_prefix: instance profile name (Default value = None)
        :return: True if exists, else False

        """
        marker = None
        while True: 
            list_instance_profiles_response = self.list_instance_profiles( path_prefix=path_prefix, marker=marker )
            for instance_profile in list_instance_profiles_response.list_instance_profiles_result.instance_profiles:
                if role_name == instance_profile.instance_profile_name: return True
            if list_instance_profiles_response.list_instance_profiles_result.is_truncated == 'true': 
                marker = list_instance_profiles_response.list_instance_profiles_result.marker
            else: break
        return False
    

class RoleInstanceProfileItem:
    """Contains attributes to manage a role - role name and policy name (note: role name and instance profile name are assumed to be exactly the same name)"""

    def __init__(self, role_name, policy_name ):
        """

        :param role_name: 
        :param policy_name: 

        """
        self.role_name = role_name
        self.policy_name = policy_name