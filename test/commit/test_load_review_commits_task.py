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

from halvemaan import commit, base, pull_request_review
from test import CaseSetup


class LoadReviewCommitsTaskTestCase(unittest.TestCase):
    """ Tests the loading of commits associated to pull request review documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit.LoadReviewCommitsTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=6, successful_tasks=6))

        # test for task complete after being run successfully
        result = luigi.build([commit.LoadReviewCommitsTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self._validate_commits(case_setup)

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review.LoadReviewsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=5, successful_tasks=5))

        # test for load after repo data is loaded
        result = luigi.build([commit.LoadReviewCommitsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        self._validate_commits(case_setup)

    def _validate_commits(self, case_setup: CaseSetup):
        reviews = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})
        overall_commit_ids = 0
        overall_commits = 0
        for review in reviews:
            if review['commit_id']:
                overall_commit_ids += 1
                actual_commit = \
                    case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.COMMIT.name,
                                                                 'id': review['commit_id']})
                self.assertEqual(1, actual_commit)
                overall_commits += actual_commit
        self.assertEqual(overall_commits, overall_commit_ids)


if __name__ == '__main__':
    unittest.main()
