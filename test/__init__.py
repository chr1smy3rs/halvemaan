# -*- coding: utf-8 -*-
#
# Copyright 2020 Chris Myers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import configparser
from datetime import timedelta, datetime

import pymongo


class CaseSetup:

    def __init__(self):
        """ init for test case setup """
        config = configparser.ConfigParser()
        config.read('luigi.cfg')

        # build the pymongo client
        client = pymongo.MongoClient(config['HalvemaanConfig']['mongo_url'])
        mongo_index = client[config['HalvemaanConfig']['mongo_index']]
        self.mongo_collection = mongo_index[config['HalvemaanConfig']['mongo_collection']]

    def cleanup_database(self):
        """ cleans up the database prior to running a test case """
        self.mongo_collection.delete_many({})

    @staticmethod
    def validate_result(luigi_result, total_tasks=0, successful_tasks=0, complete_tasks=0) -> bool:
        result = True

        total_string = f'Scheduled {total_tasks} tasks of which'
        result = result and total_string in luigi_result.summary_text

        if successful_tasks > 0:
            success_string = f'{successful_tasks} ran successfully'
            result = result and success_string in luigi_result.summary_text

        if complete_tasks > 0:
            complete_string = f'{complete_tasks} complete ones were encountered'
            result = result and complete_string in luigi_result.summary_text

        result = result and 'no failed tasks or missing dependencies' in luigi_result.summary_text

        return result

    @staticmethod
    def get_old_timestamp():
        """ create a timestamp in the past """
        ten_minutes = timedelta(minutes=10)
        timestamp = datetime.now() - ten_minutes
        timestamp = timestamp.replace(microsecond=0)
        return timestamp
