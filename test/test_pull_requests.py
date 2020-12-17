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

from halvemaan import base, pull_request
from test import CaseSetup


class LoadPullRequestsTaskTestCase(unittest.TestCase):
    """ Tests the loading of initial repository data into the mongo database """

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


if __name__ == '__main__':
    unittest.main()
