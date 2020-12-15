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
import logging
from datetime import timedelta, datetime

import pymongo
import requests

from halvemaan import actor, repository, pull_request


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
        logging.debug(f'[request] {request_json}')
        for key in self.matchers.keys():
            if key in request_json:
                logging.debug(f'[request] {request_json} [matched] {self.matchers[key]}')
                return self.matchers[key]
        # no matcher matched
        logging.error(f'[request - NOTHING matched] {request_json}')
        context.status_code = 404
        return ''

    def load_data(self):
        if "data" in self.case_json.keys():
            for item in self.case_json["data"]:
                if item["type"] == "REPOSITORY":
                    test_repo = CaseSetup.parse_repository(item)
                    self.get_mongo_collection().insert_one(test_repo.to_dictionary())
                elif item["type"] == "PULL_REQUEST":
                    test_pr = CaseSetup.parse_pull_request(item)
                    self.get_mongo_collection().insert_one(test_pr.to_dictionary())

    def get_mongo_collection(self):
        return self.mongo_collection

    @staticmethod
    def parse_repository(repository_json):

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
        test_repo.owner = actor.Actor(repository_json['owner_login'],
                                      CaseSetup.get_actor_type(repository_json['owner_type']))
        return test_repo

    @staticmethod
    def parse_pull_request(pull_request_json):

        # create a timestamp in the past
        ten_minutes = timedelta(minutes=10)
        timestamp = datetime.now() - ten_minutes
        timestamp = timestamp.replace(microsecond=0)

        test_pr = pull_request.PullRequest()
        test_pr.id = pull_request_json['id']
        test_pr.repository_id = pull_request_json['repository_id']
        test_pr.body_text = pull_request_json['body_text']
        test_pr.insert_datetime = timestamp
        test_pr.total_participants = pull_request_json['total_participants']
        test_pr.total_comments = pull_request_json['total_comments']
        test_pr.total_commits = pull_request_json['total_commits']
        test_pr.total_reactions = pull_request_json['total_reactions']
        test_pr.state = pull_request_json['state']

        # build the actor
        test_pr.author = actor.Actor(pull_request_json['author_id'],
                                    CaseSetup.get_actor_type(pull_request_json['author_type']))
        test_pr.author_association = pull_request_json['author_association']

        return test_pr

    @staticmethod
    def get_actor_type(type_str: str) -> actor.ActorType:
        for actor_type in actor.ActorType:
            if type_str == actor_type.name:
                return actor_type
        return actor.ActorType.UNKNOWN
