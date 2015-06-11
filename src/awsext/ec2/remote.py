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
Classes for remote SSH data transfer and processing
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import threading
import paramiko
import traceback
import logging
logger = logging.getLogger(__name__)


class RemoteRunThread(threading.Thread):
    """SSH into remote, SCP data to remote, run executables on remote"""

    def __init__(self, thread_num, ip_address, timeout=60, username='ec2-user', key_filename=None, remote_run_requests=None ):
        """

        :param thread_num: unique thread number, used for tracking/logging
        :param ip_address: ip address to SSH into
        :param timeout:  max time to wait for SSH connection (Default value = 60)
        :param username: user name for SSH (Default value = 'ec2-user')
        :param key_filename: path/name.ext of key file used to connect to remote (Default value = None)
        :param remote_run_requests: list of awsext.ec2.remote.RemoteRunRequest to process (Default value = None)
        :return: self.remote_run_responses contains awsext.ec2.remote.RemoteRunResponse for each processed awsext.ec2.remote.RemoteRunRequest

        """
        threading.Thread.__init__(self)
        if remote_run_requests == None: raise ValueError( "remote_run_requests is required" )
        self.thread_num = thread_num
        self.ip_address = ip_address
        self.timeout = timeout
        self.username = username
        self.key_filename = key_filename
        self.remote_run_requests = remote_run_requests
        self.remote_run_responses = []
        self.is_max_return_code_exceeded = False
        self.exception = None


    def run(self):
        """Attempt to connect to remote and process each remote_run_request """
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect( self.ip_address, timeout=self.timeout, username=self.username, key_filename=self.key_filename )
            
            for remote_run_request in self.remote_run_requests:
                std_out = ''
                std_err = ''
                std_out_lines = ''
                std_err_lines = ''
                returncode = 0
                
                if remote_run_request.from_file != None:
                    sftp_client = ssh.open_sftp()
                    sftp_client.put( remote_run_request.from_file, remote_run_request.to_file )
                    sftp_client.close()
                
                if remote_run_request.cmd_line != None:
                    chan = ssh._transport.open_session()   
                    chan.exec_command( remote_run_request.cmd_line )
                    if remote_run_request.is_wait_cmd_complete:            
                        # note that out/err doesn't have inter-stream ordering locked down.
                        std_out = chan.makefile('rb', -1)
                        std_err = chan.makefile_stderr('rb', -1)
                        std_out_lines = ''.join(std_out.readlines())
                        std_err_lines = ''.join(std_err.readlines())
                        returncode = chan.recv_exit_status()
                    chan.close()
                
                self.remote_run_responses.append( RemoteRunResponse( returncode=returncode, std_out=std_out_lines, std_err=std_err_lines, cmd_line=remote_run_request.cmd_line ))
                # max_return_code=None means "don't check the return code, i.e. don't fail on a delete during cleanup
                # if the max_return_code is specified and this execution exceeds it then stop processing
                if remote_run_request.max_returncode != None and returncode > remote_run_request.max_returncode:
                    self.is_max_return_code_exceeded = True
                    break
            
            
        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            self.exception = e
        finally:
            if ssh != None: ssh.close()
        
        
class RemoteRunRequest():
    """Contains all required values to process a remote command """
    def __init__(self, from_file=None, to_file=None, cmd_line=None, chmod_executable=False, is_wait_cmd_complete=True, max_returncode=0):
        """

        :param from_file: from path/name.ext to be SCP'ed to remote (Default value = None)
        :param to_file: target path/name.ext of file as SCP'ed to remote (Default value = None)
        :param cmd_line: command line/args to be executed on remote  (Default value = None)
        :param chmod_executable: If true, chmod +x the to_file, i.e. for a script (Default value = False)
        :param is_wait_cmd_complete: If True, wait for remote execution to complete, else start command and return immediately (Default value = True)
        :param max_returncode: max return code to check before stopping processing of requests (Default value = 0)

        """
        self.from_file = from_file
        self.to_file = to_file
        self.cmd_line = cmd_line
        self.chmod_executable = chmod_executable
        self.is_wait_cmd_complete = is_wait_cmd_complete
        self.max_returncode = max_returncode
        
        
class RemoteRunResponse():
    """Contains the results of a remote execution attempt """
    def __init__(self, returncode=0, std_out='', std_err='', cmd_line='' ):
        """

        :param returncode: return code of remote comand (Default value = 0)
        :param std_out: STD_OUT of remote command output (Default value = '')
        :param std_err: STD_ERR of remote command output (Default value = '')
        :param cmd_line: command line/args executed on remote (Default value = '')

        """
        self.returncode = returncode
        self.std_out = std_out
        self.std_err = std_err
        if cmd_line != None: self.cmd_line = cmd_line
        else: self.cmd_line = ''
        
        
        
        
        