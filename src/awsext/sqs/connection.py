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
Extensions to boto SQSConnection
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import boto.sqs.connection
import awsext.exception

class AwsExtSQSConnection(boto.sqs.connection.SQSConnection):
    """ """

    def __init__(self, **kw_params):
        """

        :param **kw_params: 

        """
        super(AwsExtSQSConnection, self).__init__(**kw_params)


    def delete_queue_sync( self, queue,  poll_interval_secs=2, poll_max_minutes=10, is_quiet=True):
        """Delete queue, wait for deletion to complete

        :param queue: Queue object
        :param poll_interval_secs: polling interval (seconds) (Default value = 2)
        :param poll_max_minutes: max minutes to wait for deletion completion (Default value = 10)
        :param is_quiet: If true, don't display poll status messages. Default value = True)

        """
        is_queue_exists = self.is_queue_exists( queue.name )
        if not is_quiet and is_queue_exists: raise awsext.exception.QueueDoesntExistError( queue.name, queue.name )
        if is_quiet and not is_queue_exists: return False   # queue doesn't exist, no reason to delete it
        is_deleted = self.delete_queue( queue )
        self.poll_queue_exists( queue.name, target_is_queue_exists=False, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        return is_deleted


    def create_queue_sync( self, queue_name, visibility_timeout=None,
                      poll_interval_secs=2, poll_max_minutes=10, ):
        """Create Queue, wait for creation completion

        :param queue_name: queue name
        :param visibility_timeout: The default visibility timeout for all messages written in the queue (Default value = None)
        :param poll_interval_secs: polling interval (seconds) (Default value = 2)
        :param poll_max_minutes: max minutes to wait for deletion completion (Default value = 10)
        :return: instance of Queue object

        """
        if self.is_queue_exists( queue_name ): raise awsext.exception.QueueAlreadyExistsError( queue_name, queue_name )
        queue = self.create_queue( queue_name, visibility_timeout=visibility_timeout )
        self.poll_queue_exists( queue_name, target_is_queue_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        return queue
            
    
    def create_unique_queue_sync( self, queue_name_prefix='q_', visibility_timeout=None,
                             max_attempts=100, poll_interval_secs=2, poll_max_minutes=10,        ):
        """Create a uniquely named Queue

        :param queue_name_prefix: queue name prefix (Default value = 'q_')
        :param visibility_timeout: The default visibility timeout for all messages written in the queue (Default value = None)
        :param max_attempts: max attempts to create unique queue name (Default value = 100)
        :param poll_interval_secs: polling for existence interval (seconds) (Default value = 2)
        :param poll_max_minutes: max minutes to poll for queue existence (Default value = 10)
        :return: instance of Queue object

        """
        unique_suffix = int(time.time()*100)
        queue = None
        for i in range(1,(max_attempts+1)):
            queue_name = queue_name_prefix + str( unique_suffix + i )
            if self.is_queue_exists( queue_name ): continue
            queue = self.create_queue( queue_name, visibility_timeout=visibility_timeout )
            break
        if queue == None: raise awsext.exception.QueueUniqueAllExistError( queue_name )
        self.poll_queue_exists( queue_name, target_is_queue_exists=True, poll_interval_secs=poll_interval_secs, poll_max_minutes=poll_max_minutes )    
        return queue
    
    
    def poll_queue_exists( self, queue_name, target_is_queue_exists=True, poll_interval_secs=2, poll_max_minutes=10 ):
        """Poll for queue exists/not exists based on value of target_is_queue_exists

        :param queue_name: param target_is_queue_exists:  (Default value = True)
        :param poll_interval_secs: Default value = 2)
        :param poll_max_minutes: Default value = 10)
        :param param target_is_queue_exists:  (Default value = True)

        """
        expires_at = time.time() + (poll_max_minutes * 60)
        while time.time() <= expires_at:
            is_queue_exists = self.is_queue_exists( queue_name )
            if is_queue_exists == target_is_queue_exists: return
            time.sleep( poll_interval_secs )
        raise awsext.exception.QueuePollTimeoutError( queue_name )
                
        
    def is_queue_exists( self, queue_name ):
        """Check for queue existence

        :param queue_name: queue name
        :return: True if queue_name exists, else False

        """
        is_queue_exists = False
        check_queues = self.get_all_queues()
        for check_queue in check_queues:
            if check_queue.name == queue_name: 
                is_queue_exists = True
                break
        return is_queue_exists
