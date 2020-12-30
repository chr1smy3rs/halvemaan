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

from halvemaan import base, pull_request_review_comment
from test import CaseSetup


class LoadReviewCommentReactionsTaskTestCase(unittest.TestCase):
    """ Tests the loading of reactions into the pull request comment documents into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review_comment.LoadReviewCommentReactionsTask(owner='google',
                                                                                         name='caliban')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=8, successful_tasks=8))

        # test for task complete after being run successfully
        result = luigi.build([pull_request_review_comment.LoadReviewCommentReactionsTask(owner='google',
                                                                                         name='caliban')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))
        self.assertTrue(LoadReviewCommentReactionsTaskTestCase._validate_reactions(case_setup))

    def test_record_in_database(self):
        """ checks for insert when repository is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review_comment.LoadReviewCommentsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=7, successful_tasks=7))

        # test for load after repo data is loaded
        result = luigi.build([pull_request_review_comment.LoadReviewCommentReactionsTask(owner='google', name='ko')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, successful_tasks=1, complete_tasks=1))
        self.assertTrue(LoadReviewCommentReactionsTaskTestCase._validate_reactions(case_setup))

    @staticmethod
    def _validate_reactions(case_setup: CaseSetup) -> bool:
        review_comments = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST_COMMENT.name})
        valid = True
        for review_comment in review_comments:
            valid = valid and review_comment['total_reactions'] == len(review_comment['reactions'])
        return valid


if __name__ == '__main__':
    unittest.main()
