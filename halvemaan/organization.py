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

import luigi

from halvemaan import base, repository, commit, user

luigi.auto_namespace(scope=__name__)


class Organization:
    """ contains the data for an organization within git """

    def __init__(self, organization_id: str):
        """
        init for an organization
        :param str organization_id: owner of the repository
        """

        self.id: str = organization_id
        self.name: str = None
        self.description: str = None
        self.object_type: base.ObjectType = base.ObjectType.ORGANIZATION

    def __str__(self) -> str:
        """
        returns a string identifying the organization
        :return: a string representation of the organization
        """
        return f'Organization [ID: {self.id}, name: {self.name}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'object_type': self.object_type.name
        }


class LoadOrganizationsForOthersTask(repository.GitMultiRepositoryTask, metaclass=abc.ABCMeta):
    """
    Task for loading organizations based on what other documents have links for
    """

    @abc.abstractmethod
    def requires(self):
        pass

    def run(self):
        for organization_id in self._find_unsaved_organizations():
            logging.debug(f'running query for organization [id: {organization_id}]')
            query = self._get_organization_query(organization_id)
            response_json = self.graph_ql_client.execute_query(query)
            logging.debug(f'query complete for organization [id: {organization_id}]')

            if response_json["data"]["node"] is not None:
                organization = Organization(organization_id)
                organization.name = response_json["data"]["node"]["name"]
                organization.description = response_json["data"]["node"]["description"]
                self._get_collection().insert_one(organization.to_dictionary())
                logging.error(f'organization [id: {organization_id}] inserted')
            else:
                logging.error(f'no organization found for id: [{organization_id}]')

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        return 0

    def _get_actual_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        return len(self._find_unsaved_organizations())

    @abc.abstractmethod
    def _find_unsaved_organizations(self) -> [str]:
        """
        returns a list of unsaved organizations
        :return:
        """
        pass

    def _is_organization_in_database(self, organization_id: str):
        logging.debug(f'running query to find organization {organization_id} in database')
        organization = self._get_collection().find_one({'id': organization_id,
                                                        'object_type': base.ObjectType.ORGANIZATION.name})
        logging.debug(f'query complete to find organization {organization_id} in database')
        return organization is not None

    @staticmethod
    def _get_organization_query(organization_id: str) -> str:

        query = """
        {
          node(id: \"""" + organization_id + """\") {
            ... on Organization {
              id
              name
              description
            }
          }
        }
        """
        return query


class LoadOrganizationsForCommitsTask(LoadOrganizationsForOthersTask):
    """
    Task for loading organizations based on what Commits are linked to
    """

    def requires(self):
        result = []
        for repo in self.repository_information["repositories"]:
            owner = repo["owner"]
            name = repo["name"]
            result.append(commit.LoadCommitsTask(owner=owner, name=name))
            result.append(commit.LoadReviewCommitsTask(owner=owner, name=name))
            result.append(commit.LoadReviewCommentCommitsTask(owner=owner, name=name))
        return result

    def _find_unsaved_organizations(self) -> [str]:
        """
        returns a list of unsaved organizations from users
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for commits')
        commits = self._get_collection().find({'object_type': 'COMMIT'})
        logging.debug(f'query for commits complete')
        for item in commits:
            if item['for_organization_id'] is not None:
                organization_id = item['for_organization_id']
                if organization_id not in result and not self._is_organization_in_database(organization_id):
                    result.add(organization_id)
        logging.debug(f'count query complete for expected organizations')
        return result

    if __name__ == '__main__':
        luigi.run()


class LoadOrganizationsForUsersTask(LoadOrganizationsForOthersTask):
    """
    Task for loading organizations based on what users are linked to
    """

    def requires(self):
        return [user.LoadUserOrganizationIdsTask(repository_information=self.repository_information)]

    def _find_unsaved_organizations(self) -> [str]:
        """
        returns a list of unsaved organizations from users
        :return:
        """
        result: {str} = set()
        logging.debug(f'running count query for expected organizations')
        logging.debug(f'running query for users')
        users = self._get_collection().find({'object_type': 'USER'})
        logging.debug(f'query for users complete')
        for item in users:
            if item['total_organizations'] > 0:
                for organization_id in item['organizations']:
                    if organization_id not in result and not self._is_organization_in_database(organization_id):
                        result.add(organization_id)
        logging.debug(f'count query complete for expected organizations')
        return result

    if __name__ == '__main__':
        luigi.run()
