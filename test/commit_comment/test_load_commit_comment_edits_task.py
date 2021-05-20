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

from halvemaan import base, commit_comment
from test import CaseSetup


class LoadCommitCommentEditsTaskTestCase(unittest.TestCase):
    """ Tests the loading of edits associated to commit comment documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([commit_comment.LoadCommitCommentEditsTask(owner='Netflix', name='lemur')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=14, successful_tasks=14))

        # test for task complete after being run successfully
        result = luigi.build([commit_comment.LoadCommitCommentEditsTask(owner='Netflix', name='lemur')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self._validate_commit_comments(case_setup)

    def _validate_commit_comments(self, case_setup: CaseSetup):
        expected_edit_count = 0
        actual_edit_count = 0
        comments = case_setup.mongo_collection.find({'object_type': base.ObjectType.COMMIT_COMMENT.name})
        for item in comments:
            expected_edit_count += item['total_edits']
            actual_edit_count += len(item['edits'])
        self.assertEqual(expected_edit_count, actual_edit_count)


if __name__ == '__main__':
    unittest.main()


