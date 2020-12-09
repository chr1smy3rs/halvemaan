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

from halvemaan import base

luigi.auto_namespace(scope=__name__)


class Repository:
    """ contains the data for a git repository """

    def __init__(self, repository_owner: str, repository_name: str):
        """
        init for a git repository
        :param str repository_owner: owner of the repository
        :param str repository_name: repository's name
        """
        self.name: str = repository_name
        self.owner: str = repository_owner
        self.id: str = None
        self.total_pull_requests: int = 0
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
            'owner': self.owner,
            'total_pull_requests': self.total_pull_requests,
            'insert_timestamp': self.insert_datetime,
            'update_timestamp': self.update_datetime,
            'object_type': self.object_type.name
        }


class GitRepositoryLookupMixin:
    """contains all of the lookup information for repository based on owner and name"""

    def _get_repository(self, owner: str, name: str) -> Repository:
        """
        returns the document for the repository
        :param str owner: the owner of the repository we are searching
        :param str name: the name of the repository we are searching
        :return: expected counts
        """
        logging.debug(f'running a query for the repository record for Repository: [owner: {owner} name: {name}] '
                      f'in database')
        found_repo = self._get_collection().find_one({'owner': owner, 'name': name, 'object_type': 'REPOSITORY'})
        logging.debug(f'query for the repository record for Repository: [owner: {owner} name: {name}] in database')
        if found_repo is not None:
            repository: Repository = Repository(found_repo['owner'], found_repo['name'])
            repository.id = found_repo['id']
            repository.total_pull_requests = found_repo['total_pull_requests']
            return repository
        else:
            return None


class GitRepositoryCountMixin:

    def _get_objects_saved_count(self, repository: Repository, object_type: base.ObjectType) -> int:
        """
        returns the expected count per repository
        :param Repository repository: the repository we are searching
        :param ObjectType object_type: the type of object we are looking for
        :return: expected counts
        """
        logging.debug(f'running count query for {object_type.name} against {repository} in database')
        count: int = self._get_collection().count({'repository_id': repository.id, 'object_type': object_type.name})
        logging.debug(f'count query complete for {object_type.name} against {repository} in database')
        return count


class GitRepositoryTask(base.GitMongoTask, GitRepositoryLookupMixin, metaclass=abc.ABCMeta):
    """ base implementation for task ran against a repository """

    owner: str = luigi.Parameter()
    name: str = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository: Repository = None

    @property
    def repository(self) -> Repository:
        if self._repository is None:
            self._repository = self._get_repository(self.owner, self.name)
        return self._repository

    @repository.setter
    def repository(self, value):
        self._repository = value


class GitMultiRepositoryTask(base.GitMongoTask, metaclass=abc.ABCMeta):
    """
    base implementation for task ran against multiple repositories
    {repositories:[{owner:<owner>, name:<name>}]}
    """

    repository_information: {} = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LoadRepositoriesTask(GitRepositoryTask, GitRepositoryLookupMixin):
    """
    Task for loading repositories from git's graphql interface
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

            saved_repository: Repository = self._get_repository(self.owner, self.name)
            if saved_repository is None:
                repository: Repository = Repository(self.owner, self.name)
                repository.id = response_json["data"]["repository"]["id"]
                repository.total_pull_requests = response_json["data"]["repository"]["pullRequests"]["totalCount"]

                logging.debug(f'inserting record for {repository}')
                self._get_collection().insert_one(repository.to_dictionary())
                logging.debug(f'insert complete for {repository}')

            else:
                total_pull_requests = response_json["data"]["repository"]["pullRequests"]["totalCount"]
                update_timestamp = datetime.now()
                set_dictionary = {'total_pull_requests': total_pull_requests, 'update_timestamp': update_timestamp}
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
        return True

    def _get_actual_results(self):
        """
        returns whether or not the repo exists
        :return: True if the repo exists
        """
        return self.repository is not None

    def _repository_query(self) -> str:
        # static method for getting the query for all the repository

        query = """
               {
                 repository(name:\"""" + self.name + """\", owner:\"""" + self.owner + """\") {
                   id
                   pullRequests (first: 1, states: MERGED) { 
                     totalCount  
                   }      
                 }
               }
               """
        return query

    if __name__ == '__main__':
        luigi.run()
