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

from halvemaan import base, pull_request_review_comment, pull_request_review
from test import CaseSetup


class LoadReviewCommentsTaskTestCase(unittest.TestCase):
    """ Tests the storing comments associated to stored pull request reviews into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review_comment.LoadReviewCommentsTask(owner='google',
                                                                                 name='cloud-forensics-utils')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=7, successful_tasks=7))

        result = luigi.build([pull_request_review_comment.LoadReviewCommentsTask(owner='google',
                                                                                 name='cloud-forensics-utils')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self._validate_comments(case_setup)

    def test_record_in_database(self):
        """ checks for insert when record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review.LoadReviewCommentIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=6, successful_tasks=6))

        result = luigi.build([pull_request_review_comment.LoadReviewCommentsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, complete_tasks=1, successful_tasks=1))
        self._validate_comments(case_setup)

    def _validate_comments(self, case_setup: CaseSetup):
        total_actual_comments = \
            case_setup.mongo_collection.count_documents({'object_type':
                                                        base.ObjectType.PULL_REQUEST_REVIEW_COMMENT.name})

        total_expected_comments = 0
        reviews = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})
        for review in reviews:
            total_expected_comments += review['total_comments']
        self.assertEqual(total_actual_comments, total_expected_comments)


if __name__ == '__main__':
    unittest.main()
