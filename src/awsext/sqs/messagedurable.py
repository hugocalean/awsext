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
Short Description
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import boto
import time
import awsext.sqs

   
import logging
logger = logging.getLogger(__name__)
    
    
class SqsMessageDurable():
    """Durable message queue - if connection drops, it's automatically restored  """
    
    def __init__(self, queue_name, region_name, profile_name=None,
                 send_attempt_max = 6, send_attempt_interval_secs = 10,
                 receive_attempt_max = 6, receive_attempt_interval_secs = 10,
                 delete_attempt_max = 6, delete_attempt_interval_secs = 10,
                 purge_attempt_max = 6, purge_attempt_interval_secs = 10,
                 ):
        """

        :param queue_name: name of queue
        :param region_name: region name
        :param profile_name: profile name from credentials file (Default value = None)
        :param send_attempt_max: max send attempts before exception (Default value = 6)
        :param send_attempt_interval_secs: sleep interval between send attempts  (Default value = 10)
        :param receive_attempt_max: max receive attempts before exception (Default value = 6)
        :param receive_attempt_interval_secs: sleep interval between receive attempts (Default value = 10)
        :param delete_attempt_max: max delete attempts before exception (Default value = 6)
        :param delete_attempt_interval_secs: sleep interval between delete attempts (Default value = 10)
        :param purge_attempt_max: max purge attempts before exception (Default value = 6)
        :param purge_attempt_interval_secs: sleep interval between purge attempts (Default value = 10)

        """
        self.queue_name = queue_name
        self.region_name = region_name
        self.profile_name = profile_name
        self.send_attempt_max = send_attempt_max
        self.send_attempt_interval_secs = send_attempt_interval_secs
        self.receive_attempt_max = receive_attempt_max
        self.receive_attempt_interval_secs = receive_attempt_interval_secs
        self.delete_attempt_max = delete_attempt_max
        self.delete_attempt_interval_secs = delete_attempt_interval_secs
        self.purge_attempt_max = purge_attempt_max
        self.purge_attempt_interval_secs = purge_attempt_interval_secs
        self.sqs_conn = None
        self.queue = None
        self.reconnect()
        
        
    def reconnect(self):
        """Attempt automatic reconnection """
        try:
            self.sqs_conn = None
            self.queue = None
            self.sqs_conn = awsext.sqs.connect_to_region( self.region_name, profile_name=self.profile_name )
            if( self.sqs_conn != None ): self.queue = self.sqs_conn.get_queue( self.queue_name )
            else: logger.warn('self.sqs_conn == None')
            if self.queue != None:
                # Long Polling
                # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
                is_set_ok = self.sqs_conn.set_queue_attribute( self.queue, 'ReceiveMessageWaitTimeSeconds', '20' )
                if not is_set_ok: logger.warn('Failed: set_queue_attribute ReceiveMessageWaitTimeSeconds')   
            else: logger.warn('self.sqs_conn == None')                
        except boto.exception.EC2ResponseError as e: 
            logger.warn( "Connection/get_queue/set_queue_attribute EC2ResponseError: " + str(e) )
            pass
        except StandardError as e:
            logger.warn( "Connection/get_queue/set_queue_attribute error: " + str(e) )
            pass


    def send_message(self, raw_text, delay_seconds=None,
                     message_attributes=None ):
        """Send message, attempt automatic reconnect/re-send on failure

        :param raw_text: raw message text
        :param message_attributes: list of message attributes (Default value = None)
        :param param delay_seconds: message delay seconds (Default value = None)

        """
        send_attempt_cnt = 0
        while True:
            try:
                self.sqs_conn.send_message( self.queue, raw_text.encode('base64'), delay_seconds=delay_seconds,
                     message_attributes=message_attributes )
                return
            except boto.exception.EC2ResponseError as e:
                logger.warn( "send_message EC2ResponseError, attempt=" + str(send_attempt_cnt) + ", error: " + str(e) )
                send_attempt_cnt += 1
                if( send_attempt_cnt > self.send_attempt_max ): raise e
            except StandardError as e:
                logger.warn( "send_message StandardError, attempt=" + str(send_attempt_cnt) + ", error: " + str(e) )
                send_attempt_cnt += 1
                if( send_attempt_cnt > self.send_attempt_max ): raise e
            
            time.sleep( self.send_attempt_interval_secs )            
            self.reconnect()


    def receive_message(self, message_attributes=None):
        """Receive single message, attempt automatic reconnect/re-receive on failure

        :param message_attributes: Default value = None)
        :return: Message instance

        """
        messages = self.receive_messages( number_messages=1, message_attributes=message_attributes )
        if len(messages) == 1: return messages[0]
        else: return None


    def receive_messages(self, number_messages=1, message_attributes=None):
        """Receive variable number of messages, attempt automatic reconnect/re-receive on failure

        :param number_messages: number of messages to receive (Default value = 1)
        :param message_attributes: list of message attribute names to be returned (Default value = None)
        :return: list of Message instances

        """
        receive_attempt_cnt = 0
        while True:
            try:
                messages = self.sqs_conn.receive_message( self.queue, number_messages=number_messages,
                                                 visibility_timeout=None, attributes=None,
                                                 wait_time_seconds=None, message_attributes=message_attributes)
                return messages
            except boto.exception.EC2ResponseError as e:
                logger.warn( "receive_messages EC2ResponseError, attempt=" + str(receive_attempt_cnt) + ", error: " + str(e) )
                receive_attempt_cnt += 1
                if( receive_attempt_cnt > self.receive_attempt_max ): raise e
            except StandardError as e:
                logger.warn( "receive_messages StandardError, attempt=" + str(receive_attempt_cnt) + ", error: " + str(e) )
                receive_attempt_cnt += 1
                if( receive_attempt_cnt > self.receive_attempt_max ): raise e
            
            time.sleep( self.receive_attempt_interval_secs )
            self.reconnect()


    def delete_message(self, message):
        """Delete a single message from the queue, attempt automatic reconnect/re-delete on failure

        :param message: 

        """
        return self.delete_messages( [message] )


    def delete_messages(self, messages):
        """Delete a list of messages from the queue, attempt automatic reconnect/re-delete on failure

        :param messages: 

        """
        delete_attempt_cnt = 0
        while True:
            try:
                while len(messages) > 0:
                    message = messages[0]
                    message.delete()
                    messages.pop(0)
                return
            except boto.exception.EC2ResponseError as e:
                logger.warn( "delete_messages EC2ResponseError, attempt=" + str(delete_attempt_cnt) + ", error: " + str(e) )
                delete_attempt_cnt += 1
                if( delete_attempt_cnt > self.delete_attempt_max ): raise e
            except StandardError as e:
                logger.warn( "delete_messages StandardError, attempt=" + str(delete_attempt_cnt) + ", error: " + str(e) )
                delete_attempt_cnt += 1
                if( delete_attempt_cnt > self.delete_attempt_max ): raise e
            
            time.sleep( self.delete_attempt_interval_secs )
            self.reconnect()

    def purge_queue(self):
        """Purge all messages from the queue, attempt automatic reconnect/re-purge on failure """
        purge_attempt_cnt = 0
        while True:
            try:
                self.sqs_conn.purge_queue( self.queue )
                return
            except boto.exception.EC2ResponseError as e:
                logger.warn( "purge_messages EC2ResponseError, attempt=" + str(purge_attempt_cnt) + ", error: " + str(e) )
                purge_attempt_cnt += 1
                if( purge_attempt_cnt > self.purge_attempt_max ): raise e
            except StandardError as e:
                logger.warn( "purge_messages StandardError, attempt=" + str(purge_attempt_cnt) + ", error: " + str(e) )
                purge_attempt_cnt += 1
                if( purge_attempt_cnt > self.purge_attempt_max ): raise e
            
            time.sleep( self.purge_attempt_interval_secs )
            self.reconnect()
