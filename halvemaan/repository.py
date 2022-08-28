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
import abc
import logging
from datetime import datetime

import luigi

from halvemaan import base, actor

luigi.auto_namespace(scope=__name__)


class Repository:
    """ contains the data for a git repository """

    def __init__(self):
        """
        init for a git repository
        """
        self.id: str = None
        self.name: str = None
        self.owner_login: str = None
        self.owner: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.total_pull_requests: int = 0
        self.pull_request_ids: [str] = []
        self.is_fork: bool = False
        self.fork_count: int = 0
        self.insert_datetime: datetime = datetime.now()
        self.update_datetime: datetime = datetime.now()
        self.object_type: base.ObjectType = base.ObjectType.REPOSITORY

    def __str__(self) -> str:
        """
        returns a string identifying the repository
        :return: a string representation of the repository
        """
        return f'Repository [name: {self.name}, owner: {self.owner}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'name': self.name,
            'owner_login': self.owner_login,
            'owner': self.owner.to_dictionary(),
            'total_pull_requests': self.total_pull_requests,
            'pull_request_ids': self.pull_request_ids,
            'is_fork': self.is_fork,
            'fork_count': self.fork_count,
            'insert_timestamp': self.insert_datetime,
            'update_timestamp': self.update_datetime,
            'object_type': self.object_type.name
        }

    @staticmethod
    def parse_dictionary(json):
        repository: Repository = Repository()
        repository.id = json['id']
        repository.owner = actor.Actor(json['owner']['id'], actor.ActorType[json['owner']['actor_type']])
        repository.owner_login = json['owner_login']
        repository.name = json['name']
        repository.total_pull_requests = json['total_pull_requests']
        repository.pull_request_ids = json['pull_request_ids']
        repository.is_fork = json['is_fork']
        repository.fork_count = json['fork_count']
        repository.insert_datetime = json['insert_timestamp']
        repository.update_datetime = json['update_timestamp']
        return repository


class GitRepositoryBasedTask(base.GitMongoTask, metaclass=abc.ABCMeta):
    """contains base operations for repository based tasks """

    def _get_repository_by_owner_and_name(self, owner_login: str, name: str) -> Repository:
        """
        returns the document for the repository
        :param str owner_login: the owner of the repository we are searching for
        :param str name: the name of the repository we are searching for
        :return: expected counts
        """
        logging.debug(f'running a query for the repository record for Repository: [owner: {owner_login} name: {name}] '
                      f'in database')
        found_repo = self._get_collection().find_one({'owner_login': owner_login, 'name': name,
                                                      'object_type': base.ObjectType.REPOSITORY.name})
        logging.debug(f'query for the repository record for '
                      f'Repository: [owner: {owner_login} name: {name}] in database')
        if found_repo is not None:
            return Repository.parse_dictionary(found_repo)
        else:
            return None

    def _get_repository_by_id(self, repo_id: str) -> Repository:
        """
        returns the document for the repository
        :param str repo_id: the id of the repository we are searching for
        :return: expected counts
        """
        logging.debug(f'running a query for the repository record for Repository: [owner: {id}] '
                      f'in database')
        found_repo = self._get_collection().find_one({'id': repo_id, 'object_type': 'REPOSITORY'})
        logging.debug(f'query for the repository record for Repository: [owner: {repo_id}] in database')
        if found_repo is not None:
            return Repository.parse_dictionary(found_repo)
        else:
            return None

    def _get_objects_saved_count(self, repository: Repository, object_type: base.ObjectType) -> int:
        # todo think about me
        """
        returns the expected count per repository
        :param Repository repository: the repository we are searching
        :param ObjectType object_type: the type of object we are looking for
        :return: expected counts
        """
        logging.debug(f'running count query for {object_type.name} against {repository} in database')
        count: int = self._get_collection().count_documents({'repository_id': repository.id,
                                                             'object_type': object_type.name})
        logging.debug(f'count query complete for {object_type.name} against {repository} in database')
        return count


class GitSingleRepositoryTask(GitRepositoryBasedTask, metaclass=abc.ABCMeta):
    """ base implementation for task ran against a single repository """

    owner: str = luigi.Parameter()
    name: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository: Repository = None

    @property
    def repository(self) -> Repository:
        if self._repository is None:
            self._repository = self._get_repository_by_owner_and_name(self.owner, self.name)
        return self._repository

    @repository.setter
    def repository(self, value):
        self._repository = value


class GitMultiRepositoryTask(GitRepositoryBasedTask, metaclass=abc.ABCMeta):
    """
    base implementation for task ran against multiple repositories
    {repositories:[{owner:<owner>, name:<name>}]}
    """

    repository_information: {} = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LoadRepositoryTask(GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading repositories from git's graphql interface

    Three test cases:
    no record, insert new
    record found, counts mismatch, update
    record found, counts match
    """

    def requires(self):
        return []

    def run(self):
        """
        loads the repository document (or updates its total count to a current value)
        :return: None
        """
        logging.debug(f'running query for Repository: [owner: {self.owner} name: {self.name}]')
        query = self._repository_query()
        response_json = self.graph_ql_client.execute_query(query)
        logging.debug(f'query complete for Repository: [owner: {self.owner} name: {self.name}]')

        if response_json["data"]["repository"] is not None:

            saved_repository: Repository = self._get_repository_by_owner_and_name(self.owner, self.name)
            if saved_repository is None:
                repository: Repository = Repository()
                repository.id = response_json["data"]["repository"]["id"]
                repository.owner_login = response_json["data"]["repository"]["owner"]["login"]
                repository.owner = self._find_actor_by_id(response_json["data"]["repository"]["owner"]["id"])
                repository.name = response_json["data"]["repository"]["name"]
                repository.is_fork = response_json["data"]["repository"]["isFork"]
                repository.fork_count = response_json["data"]["repository"]["forkCount"]
                repository.total_pull_requests = response_json["data"]["repository"]["pullRequests"]["totalCount"]

                logging.debug(f'inserting record for {repository}')
                self._get_collection().insert_one(repository.to_dictionary())
                logging.debug(f'insert complete for {repository}')

            else:
                total_pull_requests = response_json["data"]["repository"]["pullRequests"]["totalCount"]
                fork_count = response_json["data"]["repository"]["forkCount"]
                update_timestamp = datetime.now()
                set_dictionary = {'total_pull_requests': total_pull_requests, 'fork_count': fork_count,
                                  'update_timestamp': update_timestamp}
                logging.debug(f'updating record for Repository: [owner: {self.owner} name: {self.name}]')
                self._get_collection().update_one({'id': saved_repository.id}, {'$set': set_dictionary})
                logging.debug(f'updating complete for Repository: [owner: {self.owner} name: {self.name}]')

        else:
            logging.error(f'no repository returned returned for Repository: [owner: {self.owner} name: {self.name}], '
                          f'response: [{response_json}]')

        logging.debug(
            f'load for Repository: [owner: {self.owner} name: {self.name}] complete'
        )

    def _get_expected_results(self):
        """
        always expects the repo to be created
        :return: True
        """
        logging.debug(
            f'running count of expected pull requests query for Repository: [owner: {self.owner} name: {self.name}]'
        )
        query = self._repository_query()
        response_json = self.graph_ql_client.execute_query(query)
        logging.debug(
            f'count of expected pull requests query complete for Repository: [owner: {self.owner} name: {self.name}]'
        )

        if response_json["data"]["repository"] is not None:
            return response_json["data"]["repository"]["pullRequests"]["totalCount"]
        else:
            return 0

    def _get_actual_results(self):
        """
        return the current count of pull requests stored
        :return: True if the repo exists
        """
        if self.repository is not None:
            return self.repository.total_pull_requests
        else:
            return 0

    def _repository_query(self) -> str:
        # static method for getting the query for all the repository

        query = """
               {
                 repository(name:\"""" + self.name + """\", owner:\"""" + self.owner + """\") {
                   id
                    owner {
                      id
                      login
                    }
                    name
                    forkCount
                    isFork
                   pullRequests (first: 1) { 
                     totalCount  
                   }      
                 }
               }
               """
        return query

    if __name__ == '__main__':
        luigi.run()


class LoadRepositoriesByQueryTask(GitRepositoryBasedTask, actor.GitActorLookupMixin):
    """
    Task for loading repositories from git's graphql interface based on a query
    example query: language:cobol
    """

    query: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return []

    def run(self):
        """
        loads the repository document (or updates its total count to a current value)
        :return: None
        """

        repository_cursor: str = None
        has_next_page: bool = False
        if self._get_expected_results() > 0:
            has_next_page = True

        # continue executing gets against git until we have all the repositories
        while has_next_page:

            logging.debug(f'running query for Repository: [query: {self.query}]')
            search_query = self._search_query(repository_cursor)
            response_json = self.graph_ql_client.execute_query(search_query)
            logging.debug(f'query complete for Repository: [query: {self.query}]')

            if response_json["data"]["search"] is not None:

                has_next_page = response_json["data"]["search"]["pageInfo"]["hasNextPage"]

                for edge in response_json["data"]["search"]["edges"]:
                    repository_cursor = edge["cursor"]

                    owner_login = edge["node"]["owner"]["login"]
                    name = edge["node"]["name"]

                    saved_repository: Repository = self._get_repository_by_owner_and_name(owner_login, name)
                    if saved_repository is None:
                        repository: Repository = Repository()
                        repository.id = edge["node"]["id"]
                        repository.owner_login = owner_login
                        repository.owner = self._find_actor_by_id(edge["node"]["owner"]["id"])
                        repository.name = name
                        repository.is_fork = edge["node"]["isFork"]
                        repository.fork_count = edge["node"]["forkCount"]
                        repository.total_pull_requests = edge["node"]["pullRequests"]["totalCount"]

                        logging.debug(f'inserting record for {repository}')
                        self._get_collection().insert_one(repository.to_dictionary())
                        logging.debug(f'insert complete for {repository}')

                    else:
                        total_pull_requests = edge["node"]["pullRequests"]["totalCount"]
                        fork_count = edge["node"]["forkCount"]
                        update_timestamp = datetime.now()
                        set_dictionary = {'total_pull_requests': total_pull_requests, 'fork_count': fork_count,
                                          'update_timestamp': update_timestamp}
                        logging.debug(f'updating record for Repository: [owner: {owner_login} name: {name}]')
                        self._get_collection().update_one({'id': saved_repository.id}, {'$set': set_dictionary})
                        logging.debug(f'updating complete for Repository: [owner: {owner_login} name: {name}]')

    def _get_expected_results(self):
        """
        return the count of repositories expected, based on queries
        :return:
        """
        logging.debug(
            f'running count of expected repositories for Repository: [owner: {self.query}]'
        )
        search_query = self._search_query()
        response_json = self.graph_ql_client.execute_query(search_query)
        logging.debug(
            f'count of expected repositories for Repository: [owner: {self.query}]'
        )

        if response_json["data"]["search"] is not None:
            return response_json["data"]["search"]["repositoryCount"]
        else:
            return 0

    def _get_actual_results(self):
        """
        return the current count of pull requests stored
        :return: True if the repo exists
        """
        logging.debug(f'running a query for the repositories currently saved in database')
        count = self._get_collection().count_documents({'object_type': 'REPOSITORY'})
        logging.debug(f'query for the repositories currently saved in database complete')
        return count

    def _search_query(self, repository_cursor: str=None) -> str:
        """
        creates a graphql query for the repositories based on a query - asks for 100 repos a time
        :param repository_cursor: the cursor that indicates where to records were loaded until
        :return: the query string
        """

        after = ''
        if repository_cursor:
            after = ', after:"' + repository_cursor + '"'

        search_query = """
                {
                  search(query: \"""" + self.query + """\", type: REPOSITORY, first: 100""" + after + """) {
                    repositoryCount
                    pageInfo {
                      hasNextPage
                    }
                    edges { 
                      cursor
                      node {
                        ... on Repository {
                          id
                          owner {
                            id
                            login
                          }
                          name
                          forkCount
                          isFork
                          pullRequests(first: 1) {
                            totalCount
                          }
                        }
                      }
                    }
                  }
                }
               """
        return search_query

    if __name__ == '__main__':
        luigi.run()


class LoadRepositoryPullRequestIdsTask(GitSingleRepositoryTask):
    """
    Task for loading pull request ids for a single repository from git's graphql interface

    Two test cases:
    counts match, no action
    counts don't match, insert missing
    """

    def requires(self):
        return [LoadRepositoryTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the pull request for a specific repository
        :return: None
        """
        if self._get_expected_results() != self._get_actual_results():
            pull_request_cursor: str = None
            pull_request_ids: [str] = []

            # continue executing gets against git until we have all the PRs
            while self.repository.total_pull_requests > len(pull_request_ids):
                logging.debug(f'running query for pull requests against {self.repository}')
                query = self._pull_request_query(pull_request_cursor)
                response_json = self.graph_ql_client.execute_query(query)
                logging.debug(f'query complete for pull requests against {self.repository}')

                # iterate over each pull request returned (we return 20 at a time)
                for edge in response_json["data"]["repository"]["pullRequests"]["edges"]:
                    pull_request_cursor = edge["cursor"]
                    pull_request_ids.append(edge["node"]["id"])

                logging.debug(
                    f'pull requests found for {self.repository} '
                    f'{len(pull_request_ids)}/{self.repository.total_pull_requests}'
                )

            self._get_collection().update_one({'id': self.repository.id},
                                              {'$set': {'pull_request_ids': pull_request_ids,
                                                        'update_timestamp': datetime.now()}})

            actual_count: int = self._get_actual_results()
            logging.debug(
                f'pull requests returned for {self.repository} returned: [{actual_count}], '
                f'expected: [{self.repository.total_pull_requests}]'
            )

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        return self.repository.total_pull_requests

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        updated_repository = self._get_repository_by_id(self.repository.id)
        return len(updated_repository.pull_request_ids)

    def _pull_request_query(self, pull_request_cursor: str) -> str:
        """
        creates a graphql query for the pull requests within a specific repository - asks for 100 PRs a time
        :param pull_request_cursor: the cursor that indicates where to records were loaded until
        :return: the query string
        """

        after = ''
        if pull_request_cursor:
            after = 'after:"' + pull_request_cursor + '"'

        # todo configure states dynamically.
        query = """
        {
          repository(name:\"""" + self.repository.name + """\", owner:\"""" + self.repository.owner_login + """\") {
            pullRequests (first: 100, """ + after + """) { 
              edges { 
                cursor 
                node { 
                  id
                } 
              } 
            }      
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()
