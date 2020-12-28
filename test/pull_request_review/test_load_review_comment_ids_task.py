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

from halvemaan import pull_request_review, base
from test import CaseSetup


class LoadReviewCommentIdsTaskTestCase(unittest.TestCase):
    """ Tests the storing comment ids associated to stored pull request reviews into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review.LoadReviewCommentIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=6, successful_tasks=6))

        result = luigi.build([pull_request_review.LoadReviewCommentIdsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

        self.assertEqual(LoadReviewCommentIdsTaskTestCase._get_actual_reviews(case_setup),
                         LoadReviewCommentIdsTaskTestCase._get_expected_reviews(case_setup))

    def test_record_in_database(self):
        """ checks for insert when record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review.LoadReviewsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=5, successful_tasks=5))

        result = luigi.build([pull_request_review.LoadReviewCommentIdsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, complete_tasks=1, successful_tasks=1))

        self.assertEqual(LoadReviewCommentIdsTaskTestCase._get_actual_reviews(case_setup),
                         LoadReviewCommentIdsTaskTestCase._get_expected_reviews(case_setup))

    @staticmethod
    def _get_actual_reviews(case_setup: CaseSetup):
        """ find the total number of stored comment ids for pull request review documents in the database """
        total_actual_comment_ids = 0
        reviews = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})
        for review in reviews:
            total_actual_comment_ids += len(review['comment_ids'])
        return total_actual_comment_ids

    @staticmethod
    def _get_expected_reviews(case_setup: CaseSetup):
        """ find the total number of comments for pull request review documents in the database """
        total_expected_comment_ids = 0
        reviews = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})
        for review in reviews:
            total_expected_comment_ids += review['total_comments']
        return total_expected_comment_ids


if __name__ == '__main__':
    unittest.main()
