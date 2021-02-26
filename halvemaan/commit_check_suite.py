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
import logging
from datetime import datetime

import luigi

from halvemaan import base, repository, actor, commit

luigi.auto_namespace(scope=__name__)


class CheckSuite:
    """ contains the data for a user that has contributed to either a PR, review, or added a comment """

    def __init__(self, check_suite_id: str):
        self.object_type: base.ObjectType = base.ObjectType.CHECK_SUITE
        self.id: str = check_suite_id
        self.repository_id: str = None
        self.commit_id: str = None
        self.application_id: str = None
        self.branch_id: str = None
        self.total_check_runs: int = 0
        #todo change after creating check runs class
        self.check_runs: [] = []
        self.conclusion: str = None
        self.create_datetime: datetime = datetime.now()
        self.total_matching_pull_requests: int = 0
        self.matching_pull_request_ids: [str] = []
        # removing push id as it causes an error every time
        # commit [MDY6Q29tbWl0NjIwNzE2NzoxMDA3NjBmMWFlMDJmZGZkYTczNGZiNDM2ODA1MmZiNzg5ZmFlOGRm]
        # failed request [{'errors': [{'message': 'Something went wrong while executing your query.
        # Please include `C023:6502:43B2854:708AD98:5FD04E4C` when reporting this issue.'}]}]
        self.push_id: str = None
        self.state: str = None

    def __str__(self) -> str:
        """
        returns a string identifying the commit comment
        :return: a string representation of the commit comment
        """
        return f'CheckSuite [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """

        return {
            'id': self.id,
            'commit_id': self.commit_id,
            'repository_id': self.repository_id,
            'application_id': self.application_id,
            'branch_id': self.branch_id,
            'create_timestamp': self.create_datetime,
            'total_check_runs': self.total_check_runs,
            'check_runs': self.check_runs,
            'conclusion': self.conclusion,
            'total_matching_pull_requests': self.total_matching_pull_requests,
            'matching_pull_request_ids': self.matching_pull_request_ids,
            'push_id': self.push_id,
            'state': self.state,
            'object_type': self.object_type.name
        }


class LoadCommitCheckSuitesTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading check suite ids for saved commits
    """

    def requires(self):
        return [commit.LoadCommitCheckSuiteIdsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the check suite ids from the commits for a specific repository
        :return: None
        """
        commits_reviewed: int = 0

        commit_count = self._get_objects_saved_count(self.repository, base.ObjectType.COMMIT)

        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        for item in commits:
            commits_reviewed += 1

            for check_suite_id in item['check_suite_ids']:
                self._build_and_insert_commit_check_suite(check_suite_id)

            logging.debug(f'commits reviewed for {self.repository} {commits_reviewed}/{commit_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'check suites returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    def _build_and_insert_commit_check_suite(self, check_suite_id: str):

        # check to see if pull request comment is in the database
        found_request = \
            self._get_collection().find_one({'id': check_suite_id,
                                             'object_type': base.ObjectType.CHECK_SUITE.name})
        if found_request is None:
            logging.debug(
                f'running query for check suite for id [{check_suite_id}] against {self.repository}'
            )
            query = self._commit_check_suite_query(check_suite_id)
            response_json = self.graph_ql_client.execute_query(query)
            logging.debug(
                f'query complete for check suite for id [{check_suite_id}] against {self.repository}'
            )

            check_suite = CheckSuite(response_json["data"]["node"]["id"])
            check_suite.commit_id = response_json["data"]["node"]["commit"]["id"]
            check_suite.repository_id = response_json["data"]["node"]["repository"]["id"]
            if response_json["data"]["node"]["app"] is not None:
                check_suite.application_id = response_json["data"]["node"]["app"]["id"]
            if response_json["data"]["node"]["branch"] is not None:
                check_suite.branch_id = response_json["data"]["node"]["branch"]["id"]
            check_suite.conclusion = response_json["data"]["node"]["conclusion"]
            if response_json["data"]["node"]["push"] is not None:
                check_suite.push_id = response_json["data"]["node"]["push"]["id"]
            check_suite.state = response_json["data"]["node"]["status"]

            # load the counts
            check_suite.total_check_runs = response_json["data"]["node"]["checkRuns"]["totalCount"]
            check_suite.total_matching_pull_requests = \
                response_json["data"]["node"]["matchingPullRequests"]["totalCount"]

            # parse the datetime
            check_suite.create_datetime = \
                base.to_datetime_from_str(response_json["data"]["node"]["createdAt"])

            self._get_collection().insert_one(check_suite.to_dictionary())

    def _get_expected_results(self):
        """
        always find the expected number of check suites for the entire repository
        :return: 0
        """
        logging.debug(f'running count query for expected check suite ids for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        expected_count: int = 0
        for item in commits:
            expected_count += len(item['check_suite_ids'])
        logging.debug(f'count query complete for expected check suite ids for the commits in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the number of saved check suites related to commits...
        :return: integer number
        """
        logging.debug(f'running count query for actual check suite ids for the commits in {self.repository}')
        actual_count = self._get_collection().count_documents({'repository_id': self.repository.id,
                                                               'object_type': base.ObjectType.CHECK_SUITE.name})
        logging.debug(f'count query complete for actual check suite ids for the commits in {self.repository}')
        return actual_count

    @staticmethod
    def _commit_check_suite_query(check_suite_id: str) -> str:
        # static method for getting the query for a specific commit

        query = """
        {
          node(id: \"""" + check_suite_id + """\") {
            ... on CheckSuite {
              id
              app {
                id
              }
              branch {
                id
              }
              commit {
                id
              }
              checkRuns(first: 1) {
                totalCount
              }
              conclusion
              createdAt
              matchingPullRequests(first: 1) {
                totalCount
              }
              push {
                id
              }
              repository {
                id
              }
              status
            }
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()
