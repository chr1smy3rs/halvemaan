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

from halvemaan import base, pull_request, repository
from test import CaseSetup


class LoadPullRequestsTaskTestCase(unittest.TestCase):
    """ Tests the loading of pull request documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadPullRequestsTask(owner='microsoft', name='InnerEye-DeepLearning')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=3, successful_tasks=3))
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST.name})
        saved_repo = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(count, saved_repo['total_pull_requests'])

        # test for task complete after being run successfully
        result = luigi.build([pull_request.LoadPullRequestsTask(owner='microsoft', name='InnerEye-DeepLearning')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([repository.LoadRepositoryPullRequestIdsTask(owner='microsoft', name='robustdg')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=2))
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(1, count)
        saved_repo = case_setup.mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name})
        self.assertEqual(len(saved_repo['pull_request_ids']), saved_repo['total_pull_requests'])

        # test for load after repo data is loaded
        result = luigi.build([pull_request.LoadPullRequestsTask(owner='microsoft', name='robustdg')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        count = case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST.name})
        self.assertEqual(count, saved_repo['total_pull_requests'])


class LoadParticipantIdsTaskTestCase(unittest.TestCase):
    """ Tests the loading of participant ids into the pull request documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadParticipantIdsTask(owner='Netflix', name='dynomite-manager')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, successful_tasks=4))

        # test for task complete after being run successfully
        result = luigi.build([pull_request.LoadParticipantIdsTask(owner='Netflix', name='dynomite-manager')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        valid = True
        for pr in prs:
            valid = valid and pr['total_participants'] == len(pr['participants'])
        self.assertTrue(valid)

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadPullRequestsTask(owner='Netflix', name='nflxprofile')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=3, successful_tasks=3))

        # test for load after repo data is loaded
        result = luigi.build([pull_request.LoadParticipantIdsTask(owner='Netflix', name='nflxprofile')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        valid = True
        for pr in prs:
            valid = valid and pr['total_participants'] == len(pr['participants'])
        self.assertTrue(valid)


class LoadReviewIdsTaskTestCase(unittest.TestCase):
    """ Tests the loading of review ids into the pull request documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadReviewIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, successful_tasks=4))

        # test for task complete after being run successfully
        result = luigi.build([pull_request.LoadReviewIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        valid = True
        for pr in prs:
            valid = valid and pr['total_reviews'] == len(pr['review_ids'])
        self.assertTrue(valid)

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadPullRequestsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=3, successful_tasks=3))

        # test for load after repo data is loaded
        result = luigi.build([pull_request.LoadReviewIdsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        valid = True
        for pr in prs:
            valid = valid and pr['total_reviews'] == len(pr['review_ids'])
        self.assertTrue(valid)


if __name__ == '__main__':
    unittest.main()
