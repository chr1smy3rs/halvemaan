import unittest

import luigi
import mongomock
import requests_mock

from halvemaan import pull_request, base
from test import CaseSetup


class LoadPullRequestsTaskTestCase(unittest.TestCase):
    """ tests the loading of pull request data into the database based on stored ids """

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_load_one_pull_request(self, m):

        case_setup = CaseSetup('load_pull_requests', 'one_pull_request')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([pull_request.LoadPullRequestsTask(owner='test_owner', name='test_one_pull_request')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue('Scheduled 2 tasks of which' in result.summary_text)
        self.assertTrue('1 complete ones were encountered' in result.summary_text)
        self.assertTrue('1 ran successfully' in result.summary_text)
        self.assertTrue('no failed tasks or missing dependencies' in result.summary_text)
        self.assertEqual(result.status, luigi.LuigiStatusCode.SUCCESS)

        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST.name,
                                                       'id': '999999'})
        self.assertEqual(1, count)
        returned_pr = mongo_collection.find_one({'object_type': base.ObjectType.PULL_REQUEST.name, 'id': '999999'})
        self.assertEqual('999999', returned_pr['id'])

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_pull_request_loaded(self, m):

        case_setup = CaseSetup('load_pull_requests', 'one_pull_request_inserted')
        mongo_collection = case_setup.get_mongo_collection()

        m.post('http://graphql.mock.com', text=case_setup.callback)

        case_setup.load_data()

        result = luigi.build([pull_request.LoadPullRequestsTask(owner='test_owner',
                                                                name='test_one_pull_request_inserted')],
                             local_scheduler=True, detailed_summary=True)
        self.assertTrue('Scheduled 1 tasks of which' in result.summary_text)
        self.assertTrue('1 complete ones were encountered' in result.summary_text)
        self.assertTrue('no failed tasks or missing dependencies' in result.summary_text)
        self.assertEqual(result.status, luigi.LuigiStatusCode.SUCCESS)

        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.PULL_REQUEST.name,
                                                       'id': '999991'})
        self.assertEqual(1, count)
        returned_pr = mongo_collection.find_one({'object_type': base.ObjectType.PULL_REQUEST.name, 'id': '999991'})
        self.assertEqual('999991', returned_pr['id'])


if __name__ == '__main__':
    unittest.main()
