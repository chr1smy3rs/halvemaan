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

from halvemaan import commit, base
from test import CaseSetup


class LoadCommitCommentIdsTaskTestCase(unittest.TestCase):
    """ Tests the loading of commits associated to pull request documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit.LoadCommitCommentIdsTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=12, successful_tasks=12))

        # test for task complete after being run successfully
        result = luigi.build([commit.LoadCommitCommentIdsTask(owner='Netflix', name='dispatch-docker')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self._validate_commits(case_setup)

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit.LoadCommitsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=5, successful_tasks=5))

        result = luigi.build([commit.LoadReviewCommitsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, complete_tasks=1, successful_tasks=3))

        result = luigi.build([commit.LoadReviewCommentCommitsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, complete_tasks=1, successful_tasks=3))

        # test for load after repo data is loaded
        result = luigi.build([commit.LoadCommitCommentIdsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, successful_tasks=1, complete_tasks=3))
        self._validate_commits(case_setup)

    def _validate_commits(self, case_setup: CaseSetup):
        expected_comment_count = 0
        actual_comment_id_count = 0
        commits = case_setup.mongo_collection.find({'object_type': base.ObjectType.COMMIT.name})
        for item in commits:
            expected_comment_count += item['total_comments']
            actual_comment_id_count += len(item['comment_ids'])
        self.assertEqual(expected_comment_count, actual_comment_id_count)


if __name__ == '__main__':
    unittest.main()