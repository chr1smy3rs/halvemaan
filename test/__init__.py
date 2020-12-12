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
import json
from datetime import timedelta, datetime

import pymongo
import requests

from halvemaan import actor, repository


class CaseSetup:

    def __init__(self, class_code: str, case_code: str):
        json_file = open(f'setup/{class_code}.json')
        class_json = json.load(json_file)
        json_file.close()
        self.case_json = class_json[case_code]

        # load request match and corresponding response
        self.matchers: {str: str} = {}
        for item in self.case_json["matchers"]:
            self.matchers[item['match']] = json.dumps(item['response'])

        # create the mongo collection
        client = pymongo.MongoClient('mongodb://mongo.mock.com:27017/biasSandbox')
        mongo_index = client['biasSandbox']
        self.mongo_collection = mongo_index['devRawData']

    def callback(self, request: requests.Request, context):
        request_json = json.dumps(request.json())
        print(request_json)
        for key in self.matchers.keys():
            if key in request_json:
                return self.matchers[key]
        # no matcher matched
        context.status_code = 404
        return ''

    def load_data(self):
        if "data" in self.case_json.keys():
            for item in self.case_json["data"]:
                if item["type"] == "REPOSITORY":
                    test_repo = self.parse_repository(item)
                    self.get_mongo_collection().insert_one(test_repo.to_dictionary())

    def get_mongo_collection(self):
        return self.mongo_collection

    def parse_repository(self, repository_json):

        # create a timestamp in the past
        ten_minutes = timedelta(minutes=10)
        timestamp = datetime.now() - ten_minutes
        timestamp = timestamp.replace(microsecond=0)

        test_repo = repository.Repository()
        test_repo.id = repository_json['id']
        test_repo.owner_login = repository_json['owner_login']
        test_repo.name = repository_json['name']
        test_repo.total_pull_requests = repository_json['total_pull_requests']
        test_repo.insert_datetime = timestamp
        test_repo.update_datetime = timestamp

        if "pull_request_ids" in repository_json:
            test_repo.pull_request_ids = repository_json["pull_request_ids"]

        # build the actor
        for actor_type in actor.ActorType:
            if repository_json['owner_type'] == actor_type.name:
                test_repo.owner = actor.Actor(repository_json['owner_login'], actor_type)
        return test_repo
