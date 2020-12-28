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

from halvemaan import pull_request_review, pull_request, base
from test import CaseSetup


class LoadReviewsTaskTestCase(unittest.TestCase):
    """ Tests the storing reviews associated to stored pull requests into the mongo database """

    def setUp(self) -> None:
        case_setup = CaseSetup()
        case_setup.cleanup_database()

    def test_no_record_in_database(self):
        """ checks for insert when no record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request_review.LoadReviewsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=5, successful_tasks=5))

        result = luigi.build([pull_request_review.LoadReviewsTask(owner='Netflix', name='mantis')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=1, complete_tasks=1))

        self.assertEqual(LoadReviewsTaskTestCase._get_actual_reviews(case_setup),
                         LoadReviewsTaskTestCase._get_expected_reviews(case_setup))

    def test_record_in_database(self):
        """ checks for insert when record is in database """
        case_setup = CaseSetup()

        result = luigi.build([pull_request.LoadReviewIdsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=4, successful_tasks=4))

        result = luigi.build([pull_request_review.LoadReviewsTask(owner='Netflix', name='dial-reference')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue(CaseSetup.validate_result(result, total_tasks=2, complete_tasks=1, successful_tasks=1))

        self.assertEqual(LoadReviewsTaskTestCase._get_actual_reviews(case_setup),
                         LoadReviewsTaskTestCase._get_expected_reviews(case_setup))

    @staticmethod
    def _get_actual_reviews(case_setup: CaseSetup):
        """ find the total number of pull request review documents in the database """
        total_actual_reviews = \
            case_setup.mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})

        return total_actual_reviews

    @staticmethod
    def _get_expected_reviews(case_setup: CaseSetup):
        total_expected_reviews = 0
        prs = case_setup.mongo_collection.find({'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in prs:
            total_expected_reviews += pr['total_reviews']
        return total_expected_reviews


if __name__ == '__main__':
    unittest.main()
