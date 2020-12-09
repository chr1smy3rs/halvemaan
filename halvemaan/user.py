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

from halvemaan import base, repository, pull_request, pull_request_comment, pull_request_review, \
    pull_request_review_comment, commit, author, commit_comment

luigi.auto_namespace(scope=__name__)


class User:
    """ contains the data for a user that has contributed to either a PR, review, or added a comment """

    def __init__(self, login):
        """
        init for the user
        :param str login: the login for the user
        """
        self.id: str = None
        self.name: str = None
        self.login: str = login
        self.location: str = None
        self.company: str = None
        self.bio: str = None
        self.url: str = None
        self.email: str = None
        self.twitter_user_name: str = None
        self.avatar_url: str = None
        self.website_url: str = None
        self.total_organizations: int = 0
        self.organizations: [str] = []
        self.object_type: base.ObjectType = base.ObjectType.USER

    def __str__(self) -> str:
        """
        returns a string identifying the user
        :return: a string representation of the user
        """
        return f'User [id: {self.id}, login: {self.login}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'name': self.name,
            'login': self.login,
            'location': self.location,
            'company': self.company,
            'bio': self.bio,
            'url': self.url,
            'email': self.email,
            'twitter_user_name': self.twitter_user_name,
            'avatar_url': self.avatar_url,
            'website_url': self.website_url,
            'total_organizations': self.total_organizations,
            'organizations': self.organizations,
            'object_type': self.object_type.name
        }


class GitUsersTask(repository.GitRepositoryTask, metaclass=abc.ABCMeta):
    """
    Task for loading users
    """

    @abc.abstractmethod
    def requires(self):
        pass

    def run(self):
        """
        loads the users for a specific repository
        :return: None
        """

        logging.debug(
            f'running query for users in pull requests against {self.repository}'
        )
        unsaved_users = self._find_unsaved_users()
        logging.debug(
            f'query complete for users'
        )
        for unsaved_user in unsaved_users:
            user = self._find_user(unsaved_user)
            if user is not None:
                self._get_collection().insert_one(user.to_dictionary())
                logging.debug(f'record inserted for user: [{unsaved_user}: {user}]')
            else:
                logging.error(f'no user found: [{unsaved_user}]')
        logging.debug(f'query complete for users in pull requests against {self.repository}')

    def _get_expected_results(self):
        """
        always expects all users to be loaded
        :return: 0
        """
        return 0

    def _get_actual_results(self):
        """
        returns the number of unsaved users per repo...
        :return: integer number
        """
        return len(self._find_unsaved_users())

    def _is_user_in_database(self, user_id: str) -> bool:
        """
        returns the whether or not a user with that login exists in the database
        :param str user_id: the id of the user we are searching for
        :return: expected counts
        """
        logging.debug(f'running query to find user {user_id} in database')
        user = self._get_collection().find_one({'id': user_id, 'object_type': 'USER'})
        logging.debug(f'query complete to find user {user_id} in database')
        return user is not None

    def _find_user(self, unsaved_user_id: str) -> User:
        logging.debug(
            f'running query for user: [{unsaved_user_id}]'
        )
        query = self._user_query(unsaved_user_id)
        response_json = self.graph_ql_client.execute_query(query)
        logging.debug(
            f'query complete for user: [{unsaved_user_id}]'
        )
        user_json = response_json["data"]["node"]
        user = User(user_json["login"])
        user.id = user_json["id"]
        user.name = user_json["name"]
        user.location = user_json["location"]
        user.company = user_json["company"]
        user.bio = user_json["bio"]
        user.url = user_json["url"]
        user.email = user_json["email"]
        user.twitter_user_name = user_json["twitterUsername"]
        user.avatarUrl = user_json["avatarUrl"]
        user.website_url = user_json["websiteUrl"]
        user.total_organizations = user_json["organizations"]["totalCount"]
        return user

    def _add_author(self, item, result: {str}) -> {str}:
        some_author = item['author']
        if some_author is not None:
            author_id = some_author["id"]
            if some_author["author_type"] == author.AuthorType.USER.name and author_id not in result \
                    and not self._is_user_in_database(author_id):
                result.add(author_id)
        return result

    def _add_edits(self, item, result: {str}) -> {str}:
        for edit in item['edits']:
            editor = edit['editor']
            if editor is not None:
                editor_id = editor["id"]
                if editor["author_type"] == author.AuthorType.USER.name and editor_id not in result \
                        and not self._is_user_in_database(editor_id):
                    result.add(editor_id)
        return result

    def _add_reactions(self, item, result: {str}) -> {str}:
        for reaction in item['reactions']:
            reaction_author = reaction['author']
            if reaction_author is not None:
                reaction_author_id = reaction_author["id"]
                if reaction_author["author_type"] == author.AuthorType.USER.name and reaction_author_id not in result \
                        and not self._is_user_in_database(reaction_author_id):
                    result.add(reaction_author_id)
        return result

    @abc.abstractmethod
    def _find_unsaved_users(self) -> [str]:
        pass

    @staticmethod
    def _user_query(user_id: str) -> str:
        # static method for getting the user based on id

        query = """
        {
          node(id: \"""" + user_id + """\") {
            ... on User {
              id
              name
              login
              location
              company
              bio
              url
              email
              twitterUsername
              websiteUrl
              avatarUrl
              organizations(first: 1) {
                totalCount
              }
            }
          }
        }
        """
        return query


class GitSubUsersTask(GitUsersTask, metaclass=abc.ABCMeta):
    """
    Task for loading users from items underneath a pull request
    todo find a better name for this.  seriously, it sucks
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type: base.ObjectType = None

    @abc.abstractmethod
    def requires(self):
        pass

    def _find_unsaved_users(self) -> [str]:
        """
        returns a list of unsaved users from comments
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for expected users for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        for item in items:
            result = self._add_author(item, result)
            result = self._add_edits(item, result)
            result = self._add_reactions(item, result)
        logging.debug(f'count query complete for expected users for {self.object_type.name} in {self.repository}')

        return result


class LoadUsersTask(GitUsersTask):
    """
    Task for loading users from pull requests
    """

    def requires(self):
        return [pull_request.LoadPullRequestsTask(owner=self.owner, name=self.name),
                pull_request.LoadParticipantsTask(owner=self.owner, name=self.name),
                pull_request.LoadEditsTask(owner=self.owner, name=self.name),
                pull_request.LoadReactionsTask(owner=self.owner, name=self.name)]

    def _find_unsaved_users(self) -> [str]:
        """
        returns a list of unsaved users from pull requests
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for expected users for pull requests in {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in pull_requests:
            result = self._add_author(pr, result)
            for participant in pr['participants']:
                if participant is not None:
                    participant_id = participant["id"]
                    if participant["author_type"] == author.AuthorType.USER.name and participant_id not in result \
                            and not self._is_user_in_database(participant_id):
                        result.add(participant_id)
            result = self._add_edits(pr, result)
            result = self._add_reactions(pr, result)
        logging.debug(f'count query complete for expected users for pull requests in {self.repository}')

        return result

    if __name__ == '__main__':
        luigi.run()


class LoadCommentUsersTask(GitSubUsersTask):
    """
    Task for loading users from pull request comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_COMMENT

    def requires(self):
        return [pull_request_comment.LoadCommentsTask(owner=self.owner, name=self.owner),
                pull_request_comment.LoadCommentEditsTask(owner=self.owner, name=self.owner),
                pull_request_comment.LoadCommentReactionsTask(owner=self.owner, name=self.owner)]

    if __name__ == '__main__':
        luigi.run()


class LoadReviewUsersTask(GitSubUsersTask):
    """
    Task for loading users from pull request reviews
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW

    def requires(self):
        return [pull_request_review.LoadReviewsTask(owner=self.owner, name=self.name),
                pull_request_review.LoadReviewEditsTask(owner=self.owner, name=self.name),
                pull_request_review.LoadReviewReactionsTask(owner=self.owner, name=self.name)]

    if __name__ == '__main__':
        luigi.run()


class LoadReviewCommentUsersTask(GitSubUsersTask):
    """
    Task for loading users from pull request review comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW_COMMENT

    def requires(self):
        return [pull_request_review_comment.LoadReviewCommentsTask(owner=self.owner, name=self.name),
                pull_request_review_comment.LoadReviewCommentEditsTask(owner=self.owner, name=self.name),
                pull_request_review_comment.LoadReviewCommentReactionsTask(owner=self.owner, name=self.name)]

    if __name__ == '__main__':
        luigi.run()


class LoadCommitUsersTask(GitUsersTask):
    """
    Task for loading users from commits
    """

    def requires(self):
        return [commit.LoadCommitAuthorIdsTask(owner=self.owner, name=self.name)]

    def _find_unsaved_users(self) -> [str]:
        """
        returns a list of unsaved users from commits
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for expected users for commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        for item in commits:
            result = self._add_author(item, result)
            for an_author in item['authors']:
                if an_author is not None:
                    author_id = an_author["id"]
                    if an_author["author_type"] == author.AuthorType.USER.name and author_id not in result \
                            and not self._is_user_in_database(author_id):
                        result.add(author_id)
            committer = item['committer']
            if committer is not None:
                committer_id = committer["id"]
                if committer["author_type"] == author.AuthorType.USER.name and committer_id not in result \
                        and not self._is_user_in_database(committer_id):
                    result.add(committer_id)
        logging.debug(f'count query complete for expected users for commits in {self.repository}')

        return result

    if __name__ == '__main__':
        luigi.run()


class LoadCommitCommentUsersTask(GitSubUsersTask):
    """
    Task for loading users from commit comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.COMMIT_COMMENT

    def requires(self):
        return [commit_comment.LoadCommitCommentsTask(owner=self.owner, name=self.name),
                commit_comment.LoadCommitCommentEditsTask(owner=self.owner, name=self.name),
                commit_comment.LoadCommitCommentReactionsTask(owner=self.owner, name=self.name)]

    if __name__ == '__main__':
        luigi.run()


class LoadUserOrganizationIdsTask(repository.GitMultiRepositoryTask):
    """
    Task for loading links to the organizations that a user belongs to into each user
    """

    def requires(self):

        result = []
        for repo in self.repository_information["repositories"]:
            owner = repo["owner"]
            name = repo["name"]
            result.append(LoadUsersTask(owner=owner, name=name))
            result.append(LoadCommentUsersTask(owner=owner, name=name))
            result.append(LoadReviewUsersTask(owner=owner, name=name))
            result.append(LoadReviewCommentUsersTask(owner=owner, name=name))
            result.append(LoadCommitUsersTask(owner=owner, name=name))
            result.append(LoadCommitCommentUsersTask(owner=owner, name=name))

        return result

    def run(self):

        logging.debug(f'running query for users')
        users = self._get_collection().find({'object_type': 'USER'})
        logging.debug(f'query for users complete')
        for user in users:
            user_id: str = user['id']
            if user['total_organizations'] > len(user['organizations']):
                organization_ids: [str] = []
                organization_cursor: str = None
                while user['total_organizations'] > len(organization_ids):
                    logging.debug(f'running query for user [id: {user_id}]')
                    query = self._get_organization_user_query(user_id, organization_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(f'query complete for user [id: {user_id}]')

                    # iterate over each organization returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["organizations"]["edges"]:
                        organization_cursor = edge["cursor"]
                        organization_ids.append(edge["node"]["id"])

                logging.debug(f'running update for user [id: {user_id}]')
                self._get_collection().update_one({'id': user_id}, {'$set': {'organizations': organization_ids}})
                logging.debug(f'update complete for user [id: {user_id}]')

            else:
                logging.debug(f'organizations links up to date for user: [id:{user_id}]')

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected organizations for users')
        users = self._get_collection().find({'object_type': base.ObjectType.USER.name})
        expected_count: int = 0
        for user in users:
            expected_count += user['total_organizations']
        logging.debug(f'count query complete for expected organizations for users')
        return expected_count

    def _get_actual_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual organizations for users')
        users = self._get_collection().find({'object_type': base.ObjectType.USER.name})
        actual_count: int = 0
        for user in users:
            actual_count += len(user['organizations'])
        logging.debug(f'count query complete for actual organizations for users')
        return actual_count

    @staticmethod
    def _get_organization_user_query(user_id: str, organization_cursor: str) -> str:

        after = ''
        if organization_cursor:
            after = ', after:"' + organization_cursor + '", '

        query = """
        {
          node(id: \"""" + user_id + """\") {
            ... on User {
              organizations(first: 100""" + after + """) {
                edges {
                  cursor
                  node {
                    id
                  }
                }
              }
            }
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()
