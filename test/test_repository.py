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

from halvemaan import repository, base
from test import CaseSetup


class LoadRepositoryTaskTestCase(unittest.TestCase):
    """ Tests the loading of initial repository data into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record has in database """
        case_setup = CaseSetup()

        result = luigi.build([repository.LoadRepositoryTask(owner='mockito', name='mockito')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, successful_tasks=1))
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)

        result = luigi.build([repository.LoadRepositoryTask(owner='mockito', name='mockito')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)

    def test_record_in_database_counts_mismatch(self):
        """ checks for updates when the pull request counts do not match """
        case_setup = CaseSetup()

        task = repository.LoadRepositoryTask(owner='junit-team', name='junit5')
        result = luigi.build([task],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, successful_tasks=1))

        # update the repo to only one pull request
        saved_repository = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        timestamp = CaseSetup.get_old_timestamp()
        set_dictionary = {'total_pull_requests': 1, 'insert_timestamp': timestamp, 'update_timestamp': timestamp}
        case_setup.mongo_collection.update_one({'id': saved_repository['id']}, {'$set': set_dictionary})

        result = luigi.build([repository.LoadRepositoryTask(owner='junit-team', name='junit5')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, successful_tasks=1))

        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)
        saved_repository = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertTrue(saved_repository['total_pull_requests'] > 1)
        self.assertNotEqual(saved_repository['insert_timestamp'], saved_repository['update_timestamp'])


class LoadRepositoryPullRequestIdsTaskTestCase(unittest.TestCase):

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([repository.LoadRepositoryPullRequestIdsTask(owner='junit-team', name='junit5-samples')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=2))
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)

        result = luigi.build([repository.LoadRepositoryPullRequestIdsTask(owner='junit-team', name='junit5-samples')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)
        saved_repo = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(saved_repo['total_pull_requests'], len(saved_repo['pull_request_ids']))

    def test_repository_in_database(self):
        """ checks for insert when record is in database """
        case_setup = CaseSetup()

        result = luigi.build([repository.LoadRepositoryTask(owner='microsoft', name='InnerEye-DeepLearning')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, successful_tasks=1))

        result = luigi.build([repository.LoadRepositoryPullRequestIdsTask(owner='microsoft', name='InnerEye-DeepLearning')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))

        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)
        saved_repo = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(saved_repo['total_pull_requests'], len(saved_repo['pull_request_ids']))


if __name__ == '__main__':
    unittest.main()
