import json
import unittest
from datetime import datetime, timedelta

import luigi
import mongomock as mongomock
import pymongo
import requests
import requests_mock

from halvemaan import repository, base, actor


class LoadRepositoryTaskTestCase(unittest.TestCase):
    """ Tests the loading of initial repository data into the mongo database """

    @staticmethod
    def no_record_callback(request: requests.Request, context):
        request_json = json.dumps(request.json())
        if 'owner {' in request_json:
            return """
            {
              "data": {
                "repository": {
                  "id": "111111",
                  "owner": {
                    "id": "222222",
                    "login": "test_owner"
                  },
                  "name": "test_no_record",
                  "pullRequests": {
                    "totalCount": 1
                  }
                }
              }
            }
            """
        elif '__typename' in request_json:
            return """
            {
              "data": {
                "node": {
                  "__typename": "Organization",
                  "id": "222222"
                }
              }
            }
            """
        else:
            context.status_code = 404
            return ''

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_no_record_in_database(self, m):

        m.post('http://graphql.mock.com', text=self.no_record_callback)

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_no_record')],
                             local_scheduler=True)
        self.assertEqual(True, result)

        client = pymongo.MongoClient('mongodb://mongo.mock.com:27017/biasSandbox')
        mongo_index = client['biasSandbox']
        mongo_collection = mongo_index['devRawData']
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '111111'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '111111'})
        self.assertEqual('111111', returned_repo['id'])
        self.assertEqual(1, returned_repo['total_pull_requests'])

    @staticmethod
    def one_record_counts_match_callback(request: requests.Request, context):
        request_json = json.dumps(request.json())
        if 'owner {' in request_json:
            return """
            {
              "data": {
                "repository": {
                  "id": "333333",
                  "owner": {
                    "id": "222222",
                    "login": "test_owner"
                  },
                  "name": "test_one_record",
                  "pullRequests": {
                    "totalCount": 2
                  }
                }
              }
            }
            """
        elif '__typename' in request_json:
            return """
            {
              "data": {
                "node": {
                  "__typename": "Organization",
                  "id": "222222"
                }
              }
            }
            """
        else:
            context.status_code = 404
            return ''

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_record_counts_match(self, m):

        m.post('http://graphql.mock.com', text=self.one_record_counts_match_callback)

        client = pymongo.MongoClient('mongodb://mongo.mock.com:27017/biasSandbox')
        mongo_index = client['biasSandbox']
        mongo_collection = mongo_index['devRawData']

        ten_minutes = timedelta(minutes=10)
        timestamp = datetime.now() - ten_minutes
        timestamp = timestamp.replace(microsecond=0)

        test_repo = repository.Repository()
        test_repo.id = '333333'
        test_repo.owner_login = 'test_owner'
        test_repo.owner = actor.Actor('222222', actor.ActorType.ORGANIZATION)
        test_repo.name = 'test_one_record'
        test_repo.total_pull_requests = 2
        test_repo.insert_datetime = timestamp
        test_repo.update_datetime = timestamp

        mongo_collection.insert_one(test_repo.to_dictionary())

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_one_record')],
                             local_scheduler=True)
        self.assertEqual(True, result)
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '333333'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '333333'})
        self.assertEqual('333333', returned_repo['id'])
        self.assertEqual(2, returned_repo['total_pull_requests'])
        self.assertEqual(timestamp, returned_repo['insert_timestamp'])
        self.assertEqual(timestamp, returned_repo['update_timestamp'])

    @staticmethod
    def one_record_counts_mismatch_callback(request: requests.Request, context):
        request_json = json.dumps(request.json())
        if 'owner {' in request_json:
            return """
            {
              "data": {
                "repository": {
                  "id": "444444",
                  "owner": {
                    "id": "222222",
                    "login": "test_owner"
                  },
                  "name": "test_mismatch",
                  "pullRequests": {
                    "totalCount": 3
                  }
                }
              }
            }
            """
        elif '__typename' in request_json:
            return """
            {
              "data": {
                "node": {
                  "__typename": "Organization",
                  "id": "222222"
                }
              }
            }
            """
        else:
            context.status_code = 404
            return ''

    @requests_mock.Mocker()
    @mongomock.patch(servers=(('mongo.mock.com', 27017),))
    def test_one_record_counts_mismatch(self, m):

        m.post('http://graphql.mock.com', text=self.one_record_counts_mismatch_callback)

        client = pymongo.MongoClient('mongodb://mongo.mock.com:27017/biasSandbox')
        mongo_index = client['biasSandbox']
        mongo_collection = mongo_index['devRawData']

        ten_minutes = timedelta(minutes=10)
        timestamp = datetime.now() - ten_minutes
        timestamp = timestamp.replace(microsecond=0)

        test_repo = repository.Repository()
        test_repo.id = '444444'
        test_repo.owner_login = 'test_owner'
        test_repo.owner = actor.Actor('222222', actor.ActorType.ORGANIZATION)
        test_repo.name = 'test_mismatch'
        test_repo.total_pull_requests = 2
        test_repo.insert_datetime = timestamp
        test_repo.update_datetime = timestamp

        mongo_collection.insert_one(test_repo.to_dictionary())

        result = luigi.build([repository.LoadRepositoryTask(owner='test_owner', name='test_mismatch')],
                             local_scheduler=True)
        self.assertEqual(True, result)
        count: int = mongo_collection.count_documents({'object_type': base.ObjectType.REPOSITORY.name, 'id': '444444'})
        self.assertEqual(1, count)
        returned_repo = mongo_collection.find_one({'object_type': base.ObjectType.REPOSITORY.name, 'id': '444444'})
        self.assertEqual('444444', returned_repo['id'])
        self.assertEqual(3, returned_repo['total_pull_requests'])
        self.assertEqual(timestamp, returned_repo['insert_timestamp'])
        self.assertNotEqual(timestamp, returned_repo['update_timestamp'])


if __name__ == '__main__':
    unittest.main()
