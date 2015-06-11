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
Extensions to boto EC2Connection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import os
import time
import boto.ec2.connection
import awsext.exception
import awsext.ec2

import logging
logger = logging.getLogger(__name__)


class AwsExtEC2Connection(boto.ec2.connection.EC2Connection):
    """ """
    # technically the Spot Request is on hold during the until "valid_until" is passed, but in reality
    # the spot request will most likely go into "schedule-expired" state, so don't wait for it, try somewhere else
    SPOT_REQUEST_CONSTRAINTS = ['capacity-not-available','capacity-oversubscribed','price-too-low',
                                'not-scheduled-yet','launch-group-constraint','az-group-constraint',
                                'placement-group-constraint','constraint-not-fulfillable'
                           ]


    def __init__(self, **kw_params):
        """Pass through to EC2Connection constructor

        :param **kw_params: 

        """
        super(AwsExtEC2Connection, self).__init__(**kw_params)


    def delete_key_pair_sync( self, kp_name, key_path=None, max_attempts=100, poll_interval_secs=5, poll_max_minutes=10 ):
        """Delete a key pair and wait for deletion to complete or timeout

        :param kp_name: key pair name
        :param key_path: path/name.ext of file containing the key (Default value = None)
        :param max_attempts: max number of attempts to determine if the key pair has been deleted (Default value = 100)
        :param poll_interval_secs: sleep seconds between each attempt to determine if the key pair has been deleted (Default value = 5)
        :param poll_max_minutes: max minutes to determine if the key pair has been deleted (Default value = 10)

        """
        self.delete_key_pair( kp_name )          
        if key_path != None: os.remove( key_path + kp_name + '.pem' )
        # Its possible that the create_key_pair returned but the KP isn't available yet - poll for it
        self.poll_key_pair( kp_name, target_kp_exists=False )
        return


    def create_key_pair_sync( self, kp_name, key_path=None, max_attempts=100, poll_interval_secs=5, poll_max_minutes=10 ):
        """Create a key pair and wait for deletion to complete or timeout

        :param kp_name: key pair name
        :param key_path: path/name.ext of file where the key will be written (Default value = None)
        :param max_attempts: max number of attempts to determine if the key pair has been created (Default value = 100)
        :param poll_interval_secs: sleep seconds between each attempt to determine if the key pair has been created (Default value = 5)
        :param poll_max_minutes: max minutes to determine if the key pair has been created (Default value = 10)
        :return: The newly created :class:`boto.ec2.keypair.KeyPair`.

        """
        key = self.create_key_pair( kp_name )
        if key_path != None: key.save( key_path )            
        # Its possible that the create_key_pair returned but the KP isn't available yet - poll for it
        self.poll_key_pair( kp_name, target_kp_exists=True )
    
        return key


    def create_unique_key_pair( self, kp_name_prefix, key_path=None, max_attempts=100):
        """Create a key pair with a unique name based on a prefix and the current system timestamp

        :param kp_name_prefix: common prefix that will be suffixed with current system timestamp to create full kp name
        :param max_attempts: max number of attempts to determine if the key pair has been created (Default value = 100)
        :param key_path: path/name.ext of file where the key will be written (Default value = None)
        :return: The newly created :class:`boto.ec2.keypair.KeyPair`.

        """
        return self.create_unique_key_pair_sync( kp_name_prefix, key_path=key_path, max_attempts=max_attempts, is_sync=False )


    def create_unique_key_pair_sync( self, kp_name_prefix, key_path=None, 
                                max_attempts=100, poll_interval_secs=5, poll_max_minutes=10, is_sync=True ):
        """

        :param kp_name_prefix: param key_path:  (Default value = None)
        :param max_attempts: max number of attempts to determine if the key pair has been created (Default value = 100)
        :param poll_interval_secs: sleep seconds between each attempt to determine if the key pair has been created (Default value = 5)
        :param poll_max_minutes: max minutes to determine if the key pair has been created (Default value = 10)
        :param is_sync: If True, then wait for KeyPair to exist (Default value = True)
        :param key_path: path/name.ext of file where the key will be written (Default value = None)
        :return: The newly created :class:`boto.ec2.keypair.KeyPair`.

        """
        unique_suffix = int(time.time()*100)
        for i in range(1,(max_attempts+1)):
            kp_name = kp_name_prefix + str( unique_suffix )
            try:
                key = self.create_key_pair( kp_name )
                break;
            except boto.exception.EC2ResponseError as e:
                if e.error_code == 'InvalidKeyPair.Duplicate' and i<max_attempts: unique_suffix += 1
                else: raise e
        if key_path != None: key.save( key_path )
            
        # Its possible that the create_key_pair returned but the KP isn't available yet - poll for it
        if is_sync: 
            self.poll_key_pair( kp_name, target_kp_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes  )
    
        return key
    
    
    def is_key_pair_exists( self, kp_name ):
        """Check if the key pair exists

        :param kp_name: 
        :return: True if exists, False if not exists

        """
        check_key_pairs = self.get_all_key_pairs( )
        for check_key_pair in check_key_pairs: 
            if check_key_pair.name == kp_name: return True
        return False 
    
    
    def find_key_pair( self, kp_name ):
        """

        :param kp_name: 
        :return: If key exists then :class:`boto.ec2.keypair.KeyPair`, else None

        """
        check_key_pairs = self.get_all_key_pairs( )
        for check_key_pair in check_key_pairs: 
            if check_key_pair.name == kp_name: return check_key_pair
        return None 

  
    def poll_key_pair( self, kp_name, target_kp_exists=True,
                       poll_interval_secs=5, poll_max_minutes=10 ):
        """

        :param kp_name: Key pair name
        :param poll_interval_secs: Interval between check existence attempts. (Default value = 5)
        :param poll_max_minutes: Max minutes before timing out. (Default value = 10)
        :param target_kp_exists: If true, check if KeyPair exists.  If False, check if KeyPair does not exist. (Default value = True)
        :return: If target_kp_exists and KeyPair exists, then return True.  If not target_kp_exists and not KeyPair exists, return True
        :raise awsext.exception.KeyPairTimeoutError: No match on target_kp_exists within poll_max_minutes

        """
        expires_at = time.time() + (poll_max_minutes * 60)
        while time.time() <= expires_at:
            is_kp_exists = self.is_key_pair_exists( kp_name )
            if target_kp_exists == is_kp_exists: return
            time.sleep( poll_interval_secs )
        raise awsext.exception.KeyPairTimeoutError( kp_name )

    
    def is_instance_running( self, instance_id, verbose=False ):
        """Check if an instance is in awsext.ec2.INSTANCE_STATE_CODE_RUNNING state

        :param instance_id: instance id
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is running, False if not

        """
        return self.poll_instances( [instance_id], 0, 0, awsext.ec2.INSTANCE_STATE_CODE_RUNNING, verbose, is_instance_check=True )
    
    
    def is_instances_running( self, instance_ids, verbose=False ):
        """Check if a list of instances is in awsext.ec2.INSTANCE_STATE_CODE_RUNNING state

        :param instance_ids: list of instance id's
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are running, False if not

        """
        return self.poll_instances( instance_ids, 0, 0, awsext.ec2.INSTANCE_STATE_CODE_RUNNING, verbose, is_instance_check=True )
    
    
    def is_instance_stopped( self, instance_id, verbose=False ):
        """Check if an instance is in awsext.ec2.INSTANCE_STATE_CODE_STOPPED state

        :param instance_id: instance id
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is stopped, False if not

        """
        return self.poll_instances( [instance_id], 0, 0, awsext.ec2.INSTANCE_STATE_CODE_STOPPED, verbose, is_instance_check=True )
    
    
    def is_instances_stopped( self, instance_ids, verbose=False ):
        """Check if a list of instances are in awsext.ec2.INSTANCE_STATE_CODE_STOPPED state

        :param instance_ids: list of instance ids
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are running, False if not

        """
        return self.poll_instances( instance_ids, 0, 0, awsext.ec2.INSTANCE_STATE_CODE_STOPPED, verbose, is_instance_check=True )
    
   
    def is_instance_terminated( self, instance_id, verbose=False ):
        """Check if an instance is in awsext.ec2.INSTANCE_STATE_CODE_TERMINATED state

        :param instance_id: instance id
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is terminated, False if not

        """
        return self.poll_instances( [instance_id], 0, 0, awsext.ec2.INSTANCE_STATE_CODE_TERMINATED, verbose, is_instance_check=True )
    
    
    def is_instances_terminated( self, instance_ids, verbose=False ):
        """Check if a list of instances are in awsext.ec2.INSTANCE_STATE_CODE_TERMINATED state

        :param instance_ids: list of instance ids
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are terminated, False if not

        """
        return self.poll_instances( instance_ids, 0, 0, awsext.ec2.INSTANCE_STATE_CODE_TERMINATED, verbose, is_instance_check=True )

    
    def poll_instance_running( self, instance_id, interval_secs, max_minutes, verbose=False ):
        """Poll a single instance until Running or timeout

        :param instance_id: instance id
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_RUNNING
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is running
        :raise awsext.exception.InstancePollTimeoutError: if instance is not running within max_minutes

        """
        return self.poll_instances( [instance_id], interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_RUNNING, verbose )
    
    
    def poll_instances_running( self, instance_ids, interval_secs, max_minutes, verbose=False ):
        """Poll a list of instances until Running or timeout

        :param instance_ids: list of instance id's
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_RUNNING
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are running
        :raise awsext.exception.InstancePollTimeoutError: if all instances are not running within max_minutes

        """
        return self.poll_instances( instance_ids, interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_RUNNING, verbose )
    
    
    def poll_instance_stopped( self, instance_id, interval_secs, max_minutes, verbose=False ):
        """Poll a single instance until Stopped or timeout

        :param instance_id: instance id
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_STOPPED
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is stopped
        :raise awsext.exception.InstancePollTimeoutError: if instance is not stopped within max_minutes

        """
        return self.poll_instances( [instance_id], interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_STOPPED, verbose )
    
    
    def poll_instances_stopped( self, instance_ids, interval_secs, max_minutes, verbose=False ):
        """Poll a list of instances until Stopped or timeout

        :param instance_ids: list of instance id's
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_STOPPED
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are stopped
        :raise awsext.exception.InstancePollTimeoutError: if all instances are not stopped within max_minutes

        """
        return self.poll_instances( instance_ids, interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_STOPPED, verbose )
    
    
    def poll_instance_terminated( self, instance_id, interval_secs, max_minutes, verbose=False ):
        """Poll a single instance until Stopped or timeout

        :param instance_id: instance id
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_TERMINATED
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if instance is terminated
        :raise awsext.exception.InstancePollTimeoutError: if instance is not terminated within max_minutes

        """
        return self.poll_instances( [instance_id], interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_TERMINATED, verbose )
    
    
    def poll_instances_terminated( self, instance_ids, interval_secs, max_minutes, verbose=False ):
        """Poll a list of instances until Terminated or timeout

        :param instance_ids: list of instance id's
        :param max_minutes: max minutes to poll
        :param interval_secs: interval to check for status awsext.ec2.INSTANCE_STATE_CODE_TERMINATED
        :param verbose: If True, log detailed status messages. (Default value = False)
        :return: True if all instances are terminated
        :raise awsext.exception.InstancePollTimeoutError: if all instances are not terminated within max_minutes

        """
        return self.poll_instances( instance_ids, interval_secs, max_minutes, awsext.ec2.INSTANCE_STATE_CODE_TERMINATED, verbose )
    
    
    def poll_instance( self, instance_id, interval_secs, max_minutes, target_state_code, verbose=False ):
        """Common polling of instance status (target_state_code) for a given instance id

        :param instance_id: instance id
        :param interval_secs: interval to check for status target_state_code
        :param max_minutes: max minutes to poll
        :param target_state_code: awsext.ec2.INSTANCE_STATE_CODE_... to poll for, i.e. poll until awsext.ec2.INSTANCE_STATE_CODE_TERMINATED
        :param verbose: If True, log detailed status messages. Default value = False)
        :return: True if all instances are in target_state_code. 
        :raise awsext.exception.InstancePollTimeoutError: if all instances are not terminated within max_minutes

        """
        return self.poll_instances( [instance_id], interval_secs, max_minutes, target_state_code, verbose )
    
    
    def poll_instances( self, instance_ids, interval_secs, max_minutes, target_state_code, verbose=False, is_instance_check=False ):
        """Common polling of instance status (target_state_code) for a given list of instance id's

        :param instance_ids: list of instance ids's
        :param interval_secs: interval to check for status target_state_code
        :param max_minutes: max minutes to poll
        :param target_state_code: awsext.ec2.INSTANCE_STATE_CODE_... to poll for, i.e. poll until awsext.ec2.INSTANCE_STATE_CODE_TERMINATED
        :param verbose: If True, log detailed status messages. (Default value = False)
        :param is_instance_check: if True, then do a single pass (not polling), supports the is_... methods (Default value = False)
        :return: True if all instances are in target_state_code. If is_instance_check is True and instances are not in target_state_code, return False
        :raise awsext.exception.InstancePollTimeoutError: If is_instance_check is False and all instances are not in target_state_code within max_minutes

        """
        if verbose: logging.info( 'Start Polling ' + str(len(instance_ids )) + ', target_status_code=' + str(target_state_code) )
        if target_state_code == awsext.ec2.INSTANCE_STATE_CODE_RUNNING: include_all_instances = False
        else: include_all_instances = True  # by default, STOPPED or TERMINATED aren't includes
        map_poll_instance_ids = {}
        for instance_id in instance_ids: map_poll_instance_ids[instance_id] = None
        expires_at = time.time() + (max_minutes * 60)
        while True:
            if not is_instance_check and time.time() >= expires_at: break
            instance_statuss = self.get_all_instance_status(instance_ids=map_poll_instance_ids.keys(), include_all_instances=include_all_instances)
            for instance_status in instance_statuss:
                if verbose: logging.info( '   Instance: ' + instance_status.id + ', instance_status.state_code=' + str(instance_status.state_code) + ', instance_status.system_status.status=' + instance_status.system_status.status )
                # For Running, check both the State Code and Status - the instances isn't available until both are up
                if target_state_code == awsext.ec2.INSTANCE_STATE_CODE_RUNNING and instance_status.state_code == target_state_code and instance_status.system_status.status == 'ok': 
                    map_poll_instance_ids.pop( instance_status.id, None )
                # Status check isn't used for Stopped or Terminating
                elif (target_state_code == awsext.ec2.INSTANCE_STATE_CODE_TERMINATED or target_state_code == awsext.ec2.INSTANCE_STATE_CODE_STOPPED) \
                    and instance_status.state_code == target_state_code: map_poll_instance_ids.pop( instance_status.id, None )
                # This can happen with Spot instances - while waiting for checks to complete, the spot request is terminated by price
                elif( target_state_code == awsext.ec2.INSTANCE_STATE_CODE_RUNNING and instance_status.state_code == awsext.ec2.INSTANCE_STATE_CODE_TERMINATED) : 
                    raise awsext.exception.InstancePollTerminatedError( instance_status.id )
            if( len(map_poll_instance_ids) == 0 ): return True
            if is_instance_check: return False      # one pass through the loop and all of the instances are not in the target state - must be false
            if verbose: logger.info( '   Poll Loop processed, num instances remaining to target_state: ' + str(len(map_poll_instance_ids)) )
            time.sleep( interval_secs )
        
        raise awsext.exception.InstancePollTimeoutError( 'Timeout polling ' + str(map_poll_instance_ids.keys()), map_poll_instance_ids.keys(), target_state_code )


    def poll_spot_request( self, in_spot_instance_requests, interval_secs, max_minutes, 
                      cancel_and_terminate_after_timeout=True, verbose=False ):
        """Poll list of spot requests until all have been assigned an instance id, encountered a constraint or timed out

        :param in_spot_instance_requests: List of spot requests
        :param interval_secs: polling interval seconds
        :param max_minutes: max minutes until polling timeout
        :param cancel_and_terminate_after_timeout: If timeout, then Cancel the Spot Request and terminate any instance id's (Default value = True)
        :param verbose: If True, log detailed status messages. (Default value = False)

        """
        if verbose: logger.info( 'Start Polling ' + str(len(in_spot_instance_requests)) )
        instance_ids = []
        expires_at = time.time() + (max_minutes * 60)
        
        map_spot_poll_request_ids = {}
        poll_spot_request_ids = []
        for in_spot_instance_request in in_spot_instance_requests: 
            poll_spot_request_ids.append( in_spot_instance_request.id )
            map_spot_poll_request_ids[ in_spot_instance_request.id ] = None
    
        while time.time() <= expires_at:
            # Sometimes the first call will fail with Invalid Spot Request, so just try again
            try:
                poll_spot_instance_requests = self.get_all_spot_instance_requests(request_ids=map_spot_poll_request_ids.keys() )
            except boto.exception.EC2ResponseError:
                continue
            
            for poll_spot_instance_request in poll_spot_instance_requests:
                # Constraint encountered, this request probably won't succeed, will probably expire before constraints met
                if poll_spot_instance_request.status.code in self.SPOT_REQUEST_CONSTRAINTS: 
                    raise awsext.exception.SpotConstraintError(poll_spot_instance_request.status.code)
                if verbose:
                    verbose_instance_id = 'None'
                    if poll_spot_instance_request.instance_id != None: verbose_instance_id = poll_spot_instance_request.instance_id
                    logger.info( '   SpotRequest: ' + poll_spot_instance_request.id + ', instance_id=' + verbose_instance_id + ', status=' + poll_spot_instance_request.status.code + ' ' + poll_spot_instance_request.status.update_time )
                if poll_spot_instance_request.instance_id != None: 
                    map_spot_poll_request_ids.pop( poll_spot_instance_request.id, None )
                    instance_ids.append(poll_spot_instance_request.instance_id)
            if( len(map_spot_poll_request_ids) == 0 ): return instance_ids
            if verbose: logger.info( '\tPoll Loop processed, num spot requests remaining: ' + str(len(map_spot_poll_request_ids)) )
            time.sleep( interval_secs )
            
        # Timeout - optionally cancel the Request and terminate any started instances
        if cancel_and_terminate_after_timeout:
            if len(map_spot_poll_request_ids.keys()) > 0:
                self.cancel_spot_instance_requests( map_spot_poll_request_ids.keys() )
            if len(instance_ids) > 0:   
                self.terminate_instances(instance_ids )  
        
        raise awsext.exception.SpotPollTimeoutError( 'Timeout polling ' + str(map_spot_poll_request_ids.keys()), str(map_spot_poll_request_ids.keys()) )
    
     
    def get_instance_state_name_and_status( self, instance_id ):
        """For a given instance, return the State Name (i.e. initializing, running) and Status (i.e. ok)

        :param instance_id: single instance id to be checked

        """
        try: 
            instance_statuss = self.get_all_instance_status(instance_ids=[instance_id], include_all_instances=True )
            return instance_statuss[0].state_name, instance_statuss[0].system_status.status
        except StandardError as e:
            if e.error_code == 'InvalidInstanceID.NotFound': return 'not-found', 'not-applicable'
            else: raise e

