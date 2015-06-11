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
Common methods for awsext.dynamodb
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import boto


def wait_until_table_active( table, attempt_max=12, attempt_sleep_secs=5 ):
    """Create a single table and wait for creation to complete or timeout

    :param table: name of single table to create
    :param attempt_max: max number of attempts to determine if table has been created (Default value = 12)
    :param attempt_sleep_secs: sleep seconds between each attempt to determine if table has been created (Default value = 5)

    """
    wait_until_tables_active( [table], attempt_max=attempt_max, attempt_sleep_secs=attempt_sleep_secs )


def wait_until_tables_active( tables, attempt_max=12, attempt_sleep_secs=5 ):
    """Create multiple tables and wait for creation of all tables to complete or timeout

    :param tables: list of table names
    :param attempt_max: max number of attempts to determine if tables have been created (Default value = 12)
    :param attempt_sleep_secs: sleep seconds between each attempt to determine if tables have been created (Default value = 5)

    """
    attempt_cnt = 0
    pending_actives = []
    for table in tables: pending_actives.append(table)
    active_max = len(pending_actives)
    while True:
        active_num = 0
        for i in range(0, len(pending_actives)):
            table = pending_actives[i]
            if table == None: active_num += 1
            else:
                describe_table = table.describe()
                dict_table = describe_table['Table']
                if dict_table['TableStatus'] == 'ACTIVE': 
                    pending_actives[i] = None
                    active_num += 1
        if active_num == active_max: break
 
        attempt_cnt += 1
        if attempt_cnt == attempt_max: raise StandardError('Failure to achieve ACTIVE status for table:' + table.table_name )
        time.sleep( attempt_sleep_secs )


def delete_and_wait_until_table_deleted( table, attempt_max=12, attempt_sleep_secs=5 ):
    """Delete a single table and wait for deletion to complete or timeout

    :param table: name of single table to delete
    :param attempt_max: max number of attempts to determine if table has been deleted (Default value = 12)
    :param attempt_sleep_secs: sleep seconds between each attempt to determine if table has been deleted (Default value = 5)

    """
    delete_and_wait_until_tables_deleted( [table], attempt_max=attempt_max, attempt_sleep_secs=attempt_sleep_secs )


def delete_and_wait_until_tables_deleted( tables, attempt_max=12, attempt_sleep_secs=5 ):
    """Delete multiple tables and wait for deletion to complete or timeout

    :param tables: name of multiple tables to delete
    :param attempt_max: max number of attempts to determine if tables have been deleted (Default value = 12)
    :param attempt_sleep_secs: sleep seconds between each attempt to determine if tables have been deleted (Default value = 5)

    """
    pending_deletes = []
    for table in tables: pending_deletes.append(table)
    attempt_cnt = 0
    deleted_max = len(pending_deletes)
    while True:
        deleted_num = 0
        for i in range(0, len(pending_deletes)):
            table = pending_deletes[i]
            if table == None: deleted_num += 1
            else:
                try:
                    table.delete()
                except boto.exception.JSONResponseError as e:
                    if e.error_code == 'ResourceNotFoundException': 
                        pending_deletes[i] = None
                        deleted_num += 1
        if deleted_num == deleted_max: break
                
        if len(pending_deletes) == 0: break
        if attempt_cnt == attempt_max: raise e
        attempt_cnt += 1
        time.sleep( attempt_sleep_secs )
