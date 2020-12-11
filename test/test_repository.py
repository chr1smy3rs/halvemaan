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
import unittest

import luigi
import mongomock as mongomock
import requests_mock

from halvemaan import repository, base
from test import CaseSetup


class LoadRepositoryTaskTestCase(unittest.TestCase):
    """ Tests the loading of initial repository data into the mongo database """

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_no_record_in_database(self, m):

        case_setup = CaseSetup('load_repository', 'no_record')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_no_record')],
                             local_scheduler=True)
        self.assertEqual(True, result)

        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '111111'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '111111'})
        self.assertEqual('111111', returned_repo['id'])
        self.assertEqual(1, returned_repo['total_pull_requests'])

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_record_counts_match(self, m):

        case_setup = CaseSetup('load_repository', 'one_record_counts_match')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_one_record')],
                             local_scheduler=True)
        self.assertEqual(True, result)
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '333333'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '333333'})
        self.assertEqual('333333', returned_repo['id'])
        self.assertEqual(2, returned_repo['total_pull_requests'])
        self.assertEqual(returned_repo['insert_timestamp'], returned_repo['update_timestamp'])

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_record_counts_mismatch(self, m):

        case_setup = CaseSetup('load_repository', 'one_record_counts_mismatch')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_mismatch')],
                             local_scheduler=True)
        self.assertEqual(True, result)
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '444444'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '444444'})
        self.assertEqual('444444', returned_repo['id'])
        self.assertEqual(3, returned_repo['total_pull_requests'])


class LoadRepositoryPullRequestIdsTaskTestCase(unittest.TestCase):
    """ Tests the loading of pull request ids for stored repository data into the mongo database """

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_record_counts_mismatch(self, m):

        case_setup = CaseSetup('load_pull_request_ids', 'one_record_counts_mismatch')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([repository.LoadRepositoryPullRequestIdsTask(owner='test_owner',
                                                                          name='test_one_pull_request')],
                             local_scheduler=True)
        self.assertEqual(True, result)
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '555555'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '555555'})
        self.assertEqual('555555', returned_repo['id'])
        self.assertEqual(1, returned_repo['total_pull_requests'])
        self.assertEqual(1, len(returned_repo['pull_request_ids']))


if __name__ == '__main__':
    unittest.main()
