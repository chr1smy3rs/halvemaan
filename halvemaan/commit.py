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

from halvemaan import base, repository, pull_request, pull_request_review, pull_request_review_comment, actor

luigi.auto_namespace(scope=__name__)


class CommitEntry:
    """ contains the data for a entry within a commit """

    def __init__(self):
        self.id: str = None
        self.name: str = None
        self.extension: str = None
        self.path: str = None
        self.is_generated: bool = False
        self.mode: str = None
        self.type: str = None
        self.submodule_name: str = None

    def __str__(self) -> str:
        """
        override of to string
        :return: a string that represents the commit
        """
        return f'CommitEntry [id: {self.id}][path: {self.path}]'

    def to_dictionary(self) -> {}:
        """
        returns the pertinent data as a dictionary, with comments and reviews limited to only ids (they are
        other documents, after all)
        :return: a dictionary of pertinent data
        """
        return {
            'id': self.id,
            'name': self.name,
            'extension': self.extension,
            'path': self.path,
            'is_generated': self.is_generated,
            'mode': self.mode,
            'type': self.type,
            'submodule_name': self.submodule_name
        }


class Commit:
    """ contains the data for a commit """

    def __init__(self, commit_id: str):
        """
        init for commit
        :param str commit_id: the identifier for this commit
        """
        self.object_type: base.ObjectType = base.ObjectType.COMMIT
        self.id: str = commit_id
        self.repository_id: str = None
        self.author: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.authored_by_committer: bool = False
        self.total_authors: int = 0
        self.authors: [actor.Actor] = []
        self.committer: [actor.Actor] = actor.Actor('', actor.ActorType.UNKNOWN)
        # mapped to onBehalfOf
        self.for_organization_id: str = None
        # for when the code was committed
        self.create_datetime: datetime = datetime.now()
        self.push_datetime: datetime = None
        self.message_headline: str = ''
        self.message_body: str = ''
        self.total_comments: int = 0
        self.comment_ids: [str] = []
        self.total_check_suites: int = 0
        self.check_suite_ids: [str] = []
        self.tree_id: str = None
        self.entries: [CommitEntry] = []
        self.additions: int = 0
        self.deletions: int = 0
        self.changed_files: int = 0
        self.total_associated_pull_requests: int = 0
        self.associated_pull_request_ids: [str] = []
        self.state: str = None

    def __str__(self) -> str:
        """
        override of to string
        :return: a string that represents the commit
        """
        return f'Commit [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns the pertinent data as a dictionary, with comments and reviews limited to only ids (they are
        other documents, after all)
        :return: a dictionary of pertinent data
        """
        author_dictionaries = list(map(base.to_dictionary, self.authors))
        entry_dictionaries = list(map(base.to_dictionary, self.entries))
        return {
            'id': self.id,
            'repository_id': self.repository_id,
            'author': self.author.to_dictionary(),
            'authored_by_committer': self.authored_by_committer,
            'committer': self.committer.to_dictionary(),
            'total_authors': self.total_authors,
            'authors': author_dictionaries,
            'for_organization_id': self.for_organization_id,
            'create_timestamp': self.create_datetime,
            'push_timestamp': self.push_datetime,
            'message_headline': self. message_headline,
            'message_text': self.message_body,
            'total_comments': self.total_comments,
            'comment_ids': self.comment_ids,
            'total_check_suites': self.total_check_suites,
            'check_suite_ids': self.check_suite_ids,
            'tree_id': self.tree_id,
            'entries': entry_dictionaries,
            'additions': self.additions,
            'deletions': self.deletions,
            'changed_files': self.changed_files,
            'total_associated_pull_requests': self.total_associated_pull_requests,
            'associated_pull_request_ids': self.associated_pull_request_ids,
            'state': self.state,
            'object_type': self.object_type.name
        }


class GitSingleRepositoryCommitsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin,
                                     metaclass=abc.ABCMeta):
    """
    Task for loading commits
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_type: base.ObjectType = None

    @abc.abstractmethod
    def requires(self):
        pass

    def run(self):
        """
        loads the commits for a specific repository
        :return: None
        """

        logging.debug(f'running query for commits against {self.repository}')
        unsaved_commits = self._find_unsaved_commits()
        logging.debug(f'query complete for commits: {len(unsaved_commits)}')
        for unsaved_commit in unsaved_commits:
            commit = self._find_commit(unsaved_commit)
            if commit is not None:
                self._get_collection().insert_one(commit.to_dictionary())
                logging.debug(f'record inserted for commit: [{unsaved_commit}: {commit}]')
            else:
                logging.error(f'no commit found: [{unsaved_commit}]')
        logging.debug(f'query complete for commits against {self.repository}')

    def _find_commit(self, commit_id: str) -> Commit:
        logging.debug(f'running query for commit: [{commit_id}]')
        query = self._commit_query(commit_id)
        response_json = self.graph_ql_client.execute_query(query)
        logging.debug(f'query complete for commit: [{commit_id}]')

        node = response_json["data"]["node"]
        commit = Commit(node["id"])
        commit.repository_id = self.repository.id
        if node["author"] is not None:
            if node["author"]["user"] is not None:
                commit.author = self._find_actor_by_id(node["author"]["user"]["id"])
        commit.authored_by_committer = node["authoredByCommitter"]
        commit.total_authors = node["authors"]["totalCount"]
        if node["committer"] is not None:
            if node["committer"]["user"] is not None:
                commit.committer = self._find_actor_by_id(node["committer"]["user"]["id"])
        if node["onBehalfOf"] is not None:
            commit.for_organization_id = node["onBehalfOf"]["id"]
        commit.create_datetime = base.to_datetime_from_str(node["committedDate"])
        if node["pushedDate"] is not None:
            commit.push_datetime = base.to_datetime_from_str(node["pushedDate"])
        commit.total_comments = node["comments"]["totalCount"]
        commit.total_associated_pull_requests = \
            node["associatedPullRequests"]["totalCount"]
        commit.total_check_suites = node["checkSuites"]["totalCount"]
        commit.message_headline = node["messageHeadline"]
        commit.message_body = node["messageBody"]
        commit.additions = node["additions"]
        commit.deletions = node["deletions"]
        commit.changed_files = node["changedFiles"]

        # iterate over the commit tree
        commit.tree_id = node["tree"]["id"]
        for entry_node in node["tree"]["entries"]:
            entry = CommitEntry()
            entry.id = entry_node["object"]["id"]
            entry.name = entry_node["name"]
            entry.extension = entry_node["extension"]
            entry.path = entry_node["path"]
            entry.is_generated = entry_node["isGenerated"]
            entry.mode = entry_node["mode"]
            entry.type = entry_node["type"]
            if entry_node["submodule"] is not None:
                entry.submodule_name = entry_node["submodule"]["name"]
            commit.entries.append(entry)

        if node["status"] is not None:
            commit.state = node["status"]["state"]

        logging.debug(f'query complete for commit: [{commit_id}]')
        return commit

    def _get_expected_results(self):
        """
        always expects all commits to be loaded
        :return: 0
        """
        return 0

    def _get_actual_results(self):
        """
        returns the number of unsaved commit per repo...
        :return: integer number
        """
        return len(self._find_unsaved_commits())

    def _is_commit_in_database(self, commit_id: str) -> bool:
        """
        returns the whether or not a commit with that id exists in the database
        :param str commit_id: the id for the commit we are searching for
        :return: True if the commit is in the database
        """
        logging.debug(f'running query to find commit {commit_id} in database')
        commit = self._get_collection().find_one({'id': commit_id, 'object_type': 'COMMIT'})
        logging.debug(f'query complete to find commit {commit_id} in database')
        return commit is not None

    @abc.abstractmethod
    def _find_unsaved_commits(self) -> [str]:
        pass

    def _find_unsaved_commits_with_multiple_commits(self) -> [str]:
        """
        returns a list of unsaved commits from items with a list/array of commits associated to the item
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for expected commits for {self.object_type.name}  in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        for item in items:
            commit_ids = item['commit_ids']
            for commit_id in commit_ids:
                if commit_id not in result and not self._is_commit_in_database(commit_id):
                    result.add(commit_id)
        logging.debug(
            f'count query complete for expected commits for {self.object_type.name} in {self.repository}: {len(result)}'
        )
        return result

    def _find_unsaved_commits_with_single_commit(self) -> [str]:
        """
        returns a list of unsaved commits from items with a single commit associated to the item
        :return:
        """
        result: {str} = set()
        logging.debug(f'running query for unsaved commits for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        for item in items:
            commit_id = item['commit_id']
            if commit_id not in result and not self._is_commit_in_database(commit_id):
                result.add(commit_id)
        logging.debug(
            f'count query complete for unsaved  commits for {self.object_type.name} in {self.repository}: {len(result)}'
        )
        return result

    @staticmethod
    def _commit_query(commit_id: str) -> str:
        # static method for getting the query for a specific commit

        query = """
        {
          node(id: \"""" + commit_id + """\") {
            ... on Commit {
              id
              author {
                user {
                  id
                }
              }
              authoredByCommitter
              authors(first: 1) {
                totalCount
              }
              committer {
                user {
                  id
                }
              }
              onBehalfOf {
                id
              }
              comments(first: 1) {
                totalCount
              }
              associatedPullRequests(first: 1) {
                totalCount
              }
              checkSuites(first: 1) {
                totalCount
              }
              committedDate
              pushedDate
              messageHeadline
              messageBody
              additions
              deletions
              changedFiles
              tree {
                id
                entries {
                  object {
                    id
                  }
                  name
                  extension
                  path
                  isGenerated
                  mode
                  type
                  submodule {
                    name
                  }
                }
              }
              status {
                state
              }
            }
          }
        }
        """
        return query


class LoadCommitsTask(GitSingleRepositoryCommitsTask):
    """
    Task for loading commits from pull requests
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST

    def requires(self):
        return [pull_request.LoadCommitIdsTask(owner=self.owner, name=self.name)]

    def _find_unsaved_commits(self) -> [str]:
        """
        returns a list of unsaved commits from pull requests
        :return:
        """
        return self._find_unsaved_commits_with_multiple_commits()

    if __name__ == '__main__':
        luigi.run()


class LoadReviewCommitsTask(GitSingleRepositoryCommitsTask):
    """
    Task for loading commits from pull requests
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW

    def requires(self):
        return [pull_request_review.LoadReviewsTask(owner=self.owner, name=self.name)]

    def _find_unsaved_commits(self) -> [str]:
        """
        returns a list of unsaved commits from pull request reviews
        :return:
        """
        return self._find_unsaved_commits_with_single_commit()

    if __name__ == '__main__':
        luigi.run()


class LoadReviewCommentCommitsTask(GitSingleRepositoryCommitsTask):
    """
    Task for loading commits from pull request review comments
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW_COMMENT

    def requires(self):
        return [pull_request_review_comment.LoadReviewCommentsTask(owner=self.owner, name=self.name)]

    def _find_unsaved_commits(self) -> [str]:
        """
        returns a list of unsaved commits from pull request review comments
        :return:
        """
        return self._find_unsaved_commits_with_single_commit()

    if __name__ == '__main__':
        luigi.run()


class LoadCommitPullRequestIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading pull request ids for saved commits
    """

    def requires(self):
        return [LoadCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommentCommitsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the commits for a specific repository
        :return: None
        """
        commits_reviewed: int = 0

        commit_count = self._get_objects_saved_count(self.repository, base.ObjectType.COMMIT)

        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        for commit in commits:
            commit_id: str = commit['id']
            pull_requests_expected: int = commit['total_associated_pull_requests']
            pull_request_ids: [str] = []
            pull_request_cursor: str = None
            commits_reviewed += 1

            if pull_requests_expected > len(commit['associated_pull_request_ids']):
                while pull_requests_expected > len(pull_request_ids):
                    logging.debug(
                        f'running query for pull request ids for commit [{commit_id}] against {self.repository}'
                    )
                    query = self._commit_pull_request_query(commit_id, pull_request_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for pull request ids for commit [{commit_id}] against {self.repository}'
                    )

                    # iterate over each pull request returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["associatedPullRequests"]["edges"]:
                        pull_request_cursor = edge["cursor"]
                        pull_request_ids.append(edge["node"]["id"])

                self._get_collection().update_one({'id': commit_id},
                                                  {'$set': {'associated_pull_request_ids': pull_request_ids}})

            logging.debug(f'commits reviewed for {self.repository} {commits_reviewed}/{commit_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'commits returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    def _get_expected_results(self):
        """
        always find the expected number of pull request ids related to commits for the entire repository
        :return: 0
        """
        logging.debug(f'running count query for expected pull request ids for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        expected_count: int = 0
        for commit in commits:
            expected_count += commit['total_associated_pull_requests']
        logging.debug(f'count query complete for expected pull request ids for the commits in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the number of saved pull request ids associated to commits...
        :return: integer number
        """
        logging.debug(f'running count query for actual pull request ids for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        actual_count: int = 0
        for commit in commits:
            actual_count += len(commit['associated_pull_request_ids'])
        logging.debug(f'count query complete for actual pull request ids for the commits in {self.repository}')
        return actual_count

    @staticmethod
    def _commit_pull_request_query(commit_id: str, pull_request_cursor: str) -> str:
        # static method for getting the query for a specific commit

        after = ''
        if pull_request_cursor:
            after = ', after:"' + pull_request_cursor + '", '

        query = """
        {
          node(id: \"""" + commit_id + """\") {
            ... on Commit {
              associatedPullRequests(first: 100""" + after + """) {
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


class LoadCommitActorIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading author ids for saved commits
    """

    def requires(self):
        return [LoadCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommentCommitsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the authors for the commits for a specific repository
        :return: None
        """
        commits_reviewed: int = 0

        commit_count = self._get_objects_saved_count(self.repository, base.ObjectType.COMMIT)

        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        for commit in commits:
            commit_id: str = commit['id']
            authors_expected: int = commit['total_authors']
            authors: [actor.Actor] = []
            author_cursor: str = None
            commits_reviewed += 1

            if authors_expected > len(commit['authors']):
                while authors_expected > len(authors):
                    logging.debug(
                        f'running query for authors for commit [{commit_id}] against {self.repository}'
                    )
                    query = self._commit_authors_query(commit_id, author_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for authors for commit [{commit_id}] against {self.repository}'
                    )

                    # iterate over each author returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["authors"]["edges"]:
                        author_cursor = edge["cursor"]
                        if edge["node"]["user"] is not None:
                            authors.append(self._find_actor_by_id(edge["node"]["user"]["id"]))
                        else:
                            authors.append(actor.Actor('', actor.ActorType.UNKNOWN))

                author_dictionaries = list(map(base.to_dictionary, authors))
                self._get_collection().update_one({'id': commit_id},
                                                  {'$set': {'authors': author_dictionaries}})

            logging.debug(f'pull requests reviewed for {self.repository} {commits_reviewed}/{commit_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'authors returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    def _get_expected_results(self):
        """
        always find the expected number of authors for the entire repository
        :return: 0
        """
        logging.debug(f'running count query for expected author ids for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        expected_count: int = 0
        for commit in commits:
            expected_count += commit['total_authors']
        logging.debug(f'count query complete for expected author ids for the commits in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the number of saved author ids...
        :return: integer number
        """
        logging.debug(f'running count query for actual author ids for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        actual_count: int = 0
        for commit in commits:
            actual_count += len(commit['authors'])
        logging.debug(f'count query complete for actual author ids for the commits in {self.repository}')
        return actual_count

    @staticmethod
    def _commit_authors_query(commit_id: str, authors_cursor: str) -> str:
        # static method for getting the query for a specific commit

        after = ''
        if authors_cursor:
            after = ', after:"' + authors_cursor + '", '

        query = """
        {
          node(id: \"""" + commit_id + """\") {
            ... on Commit {
              authors(first: 100) {
                edges {
                  cursor
                  node {
                    user{
                      id
                    }
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


class LoadCommitCommentIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading comment ids for saved commits
    """

    def requires(self):
        return [LoadCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommitsTask(owner=self.owner, name=self.name),
                LoadReviewCommentCommitsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the comments for the commits for a specific repository
        :return: None
        """
        commits_reviewed: int = 0

        commit_count = self._get_objects_saved_count(self.repository, base.ObjectType.COMMIT)

        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        for item in commits:
            commit_id: str = item['id']
            comments_expected: int = item['total_comments']
            comment_ids: [str] = []
            comment_cursor: str = None
            commits_reviewed += 1

            if comments_expected > len(item['comment_ids']):
                while comments_expected > len(comment_ids):
                    logging.debug(
                        f'running query for comments for commit [{commit_id}] against {self.repository}'
                    )
                    query = self._commit_comment_query(commit_id, comment_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for comments for commit [{commit_id}] against {self.repository}'
                    )

                    # iterate over each participant returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["comments"]["edges"]:
                        comment_cursor = edge["cursor"]
                        comment_ids.append(edge["node"]["id"])

                self._get_collection().update_one({'id': commit_id},
                                                  {'$set': {'comment_ids': comment_ids}})

            logging.debug(f'pull requests reviewed for {self.repository} {commits_reviewed}/{commit_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'participants returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    def _get_expected_results(self):
        """
        always find the expected number of commit comments for the entire repository
        :return: 0
        """
        logging.debug(f'running count query for expected comments for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        expected_count: int = 0
        for item in commits:
            expected_count += item['total_comments']
        logging.debug(f'count query complete for expected comments for the commits in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        always find the actual number of commit comments for the entire repository
        :return: 0
        """
        logging.debug(f'running count query for actual comments for the commits in {self.repository}')
        commits = self._get_collection().find({'repository_id': self.repository.id,
                                               'object_type': base.ObjectType.COMMIT.name})
        actual_count: int = 0
        for item in commits:
            actual_count += len(item['comment_ids'])
        logging.debug(f'count query complete for actual comments for the commits in {self.repository}')
        return actual_count

    @staticmethod
    def _commit_comment_query(commit_id: str, comment_cursor: str) -> str:
        # static method for getting the query for a specific commit

        after = ''
        if comment_cursor:
            after = ', after:"' + comment_cursor + '", '

        query = """
        {
          node(id: \"""" + commit_id + """\") {
            ... on Commit {
              comments(first: 100""" + after + """) {
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
