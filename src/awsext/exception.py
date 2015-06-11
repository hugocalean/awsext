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
Exceptions for awsext
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

class InstancePollTimeoutError(Exception):
    """ """

    def __init__(self, message, instance_ids, target_status_code ):
        """

        :param message: 
        :param instance_ids: 
        :param target_status_code: 

        """
        super(InstancePollTimeoutError, self).__init__(message)
        self.instance_ids = instance_ids
        self.target_status_code = target_status_code


class InstancePollTerminatedError(Exception):
    """ """

    def __init__(self, message, instance_id ):
        """

        :param message: 
        :param instance_id: 

        """
        super(InstancePollTimeoutError, self).__init__(message)
        self.instance_id = instance_id


class KeyPairTimeoutError(Exception):
    """ """

    def __init__(self, message, kp_name ):
        """

        :param message: 
        :param kp_name: 

        """
        super(KeyPairTimeoutError, self).__init__(message)
        self.kp_name = kp_name


class InstanceProfileTimeoutError(Exception):
    """ """

    def __init__(self, message, instance_profile_name, create_or_delete='create' ):
        """

        :param message: 
        :param instance_profile_name: 
        :param create_or_delete:  (Default value = 'create')

        """
        super(InstanceProfileTimeoutError, self).__init__(message)
        self.instance_profile_name = instance_profile_name
        self.create_or_delete = create_or_delete
        

class RoleTimeoutError(Exception):
    """ """

    def __init__(self, message ):
        """

        :param message: 

        """
        super(RoleTimeoutError, self).__init__(message)
        

class InstanceTimeoutError(Exception):
    """ """

    def __init__(self, message ):
        """

        :param message: 

        """
        super(InstanceTimeoutError, self).__init__(message)
        

class SpotPollTimeoutError(Exception):
    """ """

    def __init__(self, message, spot_request_ids ):
        """

        :param message: 
        :param spot_request_ids: 

        """
        super(SpotPollTimeoutError, self).__init__(message)
        self.spot_request_ids = spot_request_ids
        

class SpotInstanceStateNameAndStatusError(Exception):
    """ """

    def __init__(self, message, state_name, status ):
        """

        :param message: 
        :param state_name: 
        :param status: 

        """
        super(SpotPollTimeoutError, self).__init__(message)
        self.state_name = state_name
        self.status = status
        

class SpotConstraintError(Exception):
    """ """

    def __init__(self, message ):
        """

        :param message: 

        """
        super(SpotConstraintError, self).__init__(message)
        

class SpotRequestIdNotFoundError(Exception):
    """ """

    def __init__(self, message ):
        """

        :param message: 

        """
        super(SpotConstraintError, self).__init__(message)


class SecurityGroupTimeoutError(Exception):
    """ """

    def __init__(self, message, security_group_id ):
        """

        :param message: 
        :param security_group_id: 

        """
        super(SecurityGroupTimeoutError, self).__init__(message)
        self.security_group_id = security_group_id


class SecurityGroupDoesntExistError(Exception):
    """ """

    def __init__(self, message, security_group_id ):
        """

        :param message: 
        :param security_group_id: 

        """
        super(SecurityGroupDoesntExistError, self).__init__(message)
        self.security_group_id = security_group_id


class SecurityGroupAlreadyExistsError(Exception):
    """ """

    def __init__(self, message, security_group_id ):
        """

        :param message: 
        :param security_group_id: 

        """
        super(SecurityGroupAlreadyExistsError, self).__init__(message)
        self.security_group_id = security_group_id


class QueuePollTimeoutError(Exception):
    """ """

    def __init__(self, message, queue_name ):
        """

        :param message: 
        :param queue_name: 

        """
        super(QueuePollTimeoutError, self).__init__(message)
        self.queue_name = queue_name


class QueueUniqueAllExistError(Exception):
    """ """

    def __init__(self, message ):
        """

        :param message: 

        """
        super(QueueUniqueAllExistError, self).__init__(message)


class QueueAlreadyExistsError(Exception):
    """ """

    def __init__(self, message, queue ):
        """

        :param message: 
        :param queue: 

        """
        super(QueueAlreadyExistsError, self).__init__(message)
        self.queue = queue


class QueueDoesntExistError(Exception):
    """ """

    def __init__(self, message, queue ):
        """

        :param message: 
        :param queue: 

        """
        super(QueueDoesntExistError, self).__init__(message)
        self.queue = queue
