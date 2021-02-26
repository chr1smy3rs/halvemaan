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

from halvemaan import pull_request, commit, base, commit_check_suite
from test import CaseSetup


class LoadCommitCommentsTaskTestCase(unittest.TestCase):
    """ Tests the loading of commits associated to pull request documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit_check_suite.LoadCommitCheckSuitesTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=13, successful_tasks=13))

        # test for task complete after being run successfully
        result = luigi.build([commit_check_suite.LoadCommitCheckSuitesTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self._validate_commits(case_setup)

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit.LoadCommitCheckSuiteIdsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=12, successful_tasks=12))

        # test for load after repo data is loaded
        result = luigi.build([commit_check_suite.LoadCommitCheckSuitesTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        self._validate_commits(case_setup)

    def _validate_commits(self, case_setup: CaseSetup):
        expected_pull_request_count = 0
        actual_pull_request_id_count = 0
        commits = case_setup.mongo_collection.find({'object_type': base.ObjectType.COMMIT.name})
        for item in commits:
            expected_pull_request_count += item['total_check_suites']
            actual_pull_request_id_count += len(item['check_suite_ids'])
        self.assertEqual(expected_pull_request_count, actual_pull_request_id_count)


if __name__ == '__main__':
    unittest.main()

