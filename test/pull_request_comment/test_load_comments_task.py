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

from halvemaan import base, pull_request_comment, pull_request
from test import CaseSetup


class LoadCommentsTaskTestCase(unittest.TestCase):
    """ Tests the storing comments associated to stored pull requests into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_comment.LoadCommentsTask(owner='google', name='cloud-forensics-utils')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=5, successful_tasks=5))

        result = luigi.build([pull_request_comment.LoadCommentsTask(owner='google', name='cloud-forensics-utils')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

        self.assertEqual(LoadCommentsTaskTestCase._get_actual_comments(case_setup),
                         LoadCommentsTaskTestCase._get_expected_comments(case_setup))

    def test_record_in_database(self):
        """ checks for insert when record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadCommentIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, successful_tasks=4))

        result = luigi.build([pull_request_comment.LoadCommentsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, complete_tasks=1, successful_tasks=1))

        self.assertEqual(LoadCommentsTaskTestCase._get_actual_comments(case_setup),
                         LoadCommentsTaskTestCase._get_expected_comments(case_setup))

    @staticmethod
    def _get_actual_comments(case_setup: CaseSetup):
        """ find the total number of pull request comment documents in the database """
        total_actual_comments = \
            case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST_COMMENT.name})

        return total_actual_comments

    @staticmethod
    def _get_expected_comments(case_setup: CaseSetup):
        total_expected_comments = 0
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in prs:
            total_expected_comments += pr['total_comments']
        return total_expected_comments


if __name__ == '__main__':
    unittest.main()
