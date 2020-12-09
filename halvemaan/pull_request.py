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

from halvemaan import base, user, repository, content, author

luigi.auto_namespace(scope=__name__)


class PullRequest:
    """ contains the data for a pull request """

    def __init__(self, request_id: str, repository_id: str):
        """
        init for pull request
        :param str request_id: the identifier for this request
        :param str repository_id: the id of the repository this request belongs to
        """
        self.object_type: base.ObjectType = base.ObjectType.PULL_REQUEST
        self.id: str = request_id
        self.repository_id: str = repository_id
        self.author: author.Author = author.Author('', author.AuthorType.UNKNOWN)
        self.author_association: str = None
        self.create_datetime: datetime = datetime.now()
        self.body_text: str = ''
        self.total_participants: int = 0
        self.participants: [author.Author] = []
        self.total_comments: int = 0
        self.comment_ids: [str] = []
        self.total_reviews: int = 0
        self.review_ids: [str] = []
        self.total_commits: int = 0
        self.commit_ids: [str] = []
        self.total_edits: int = 0
        self.edits: [content.ContentEdit] = []
        self.total_reactions: int = 0
        self.reactions: [content.Reaction] = []
        self.state: str = None

    def __str__(self) -> str:
        """
        override of to string
        :return: a string that represents the pull request
        """
        return f'PullRequest [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns the pertinent data as a dictionary, with comments and reviews limited to only ids (they are
        other documents, after all)
        :return: a dictionary of pertinent data
        """
        reaction_dictionaries = list(map(base.to_dictionary, self.reactions))
        edit_dictionaries = list(map(base.to_dictionary, self.edits))
        participant_dictionaries = list(map(base.to_dictionary, self.participants))
        return {
            'id': self.id,
            'repository_id': self.repository_id,
            'author': self.author.to_dictionary(),
            'author_association': self.author_association,
            'create_timestamp': self.create_datetime,
            'text': self.body_text,
            'total_participants': self.total_participants,
            'participants': participant_dictionaries,
            'total_comments': self.total_comments,
            'comment_ids': self.comment_ids,
            'total_reviews': self.total_reviews,
            'review_ids': self.review_ids,
            'total_commits': self.total_commits,
            'commit_ids': self.commit_ids,
            'total_edits': self.total_edits,
            'edits': edit_dictionaries,
            'total_reactions': self.total_reactions,
            'reactions': reaction_dictionaries,
            'state': self.state,
            'object_type': self.object_type.name
        }


class LoadPullRequestsTask(repository.GitRepositoryTask, author.GitAuthorLookupMixin, repository.GitRepositoryCountMixin):
    """
    Task for loading pull requests from git's graphql interface
    """

    def requires(self):
        return [repository.LoadRepositoriesTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the pull request for a specific repository
        :return: None
        """
        if self._get_expected_results() != self._get_actual_results():
            pull_requests_loaded: int = 0
            pull_request_cursor: str = None

            # continue executing gets against git until we have all the PRs
            while self.repository.total_pull_requests > pull_requests_loaded:
                logging.debug(f'running query for pull requests against {self.repository}')
                query = self._pull_request_query(pull_request_cursor)
                response_json = self.graph_ql_client.execute_query(query)
                logging.debug(f'query complete for pull requests against {self.repository}')

                # iterate over each pull request returned (we return 20 at a time)
                for edge in response_json["data"]["repository"]["pullRequests"]["edges"]:
                    pull_requests_loaded += 1
                    pull_request_cursor = edge["cursor"]
                    pull_request_id = edge["node"]["id"]

                    # check to see if pull request is in the database
                    found_request = self._get_collection().find_one({'id': pull_request_id,
                                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
                    if found_request is None:

                        pr = PullRequest(pull_request_id, self.repository.id)
                        pr.body_text = edge["node"]["bodyText"]
                        pr.state = edge["node"]["state"]

                        # load the counts
                        pr.total_reviews = edge["node"]["reviews"]["totalCount"]
                        pr.total_comments = edge["node"]["comments"]["totalCount"]
                        pr.total_participants = edge["node"]["participants"]["totalCount"]
                        pr.total_edits = edge["node"]["userContentEdits"]["totalCount"]
                        pr.total_reactions = edge["node"]["reactions"]["totalCount"]
                        pr.total_commits = edge["node"]["commits"]["totalCount"]

                        # author can be None.  Who knew?
                        if edge["node"]["author"] is not None:
                            pr.author = self._find_author_by_login(edge["node"]["author"]["login"])
                        pr.author_association = pr.author_login = edge["node"]["authorAssociation"]

                        # parse the datetime
                        pr.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])

                        logging.debug(f'inserting record for {pr}')
                        self._get_collection().insert_one(pr.to_dictionary())
                        logging.debug(f'insert complete for {pr}')
                    else:
                        logging.debug(f'Pull Request [id: {pull_request_id}] already found in database')

                logging.debug(
                    f'pull requests found for {self.repository} '
                    f'{pull_requests_loaded}/{self.repository.total_pull_requests}'
                )

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
        return self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

    def _pull_request_query(self, pull_request_cursor: str) -> str:
        """
        creates a graphql query for the pull requests within a specific repository - asks for 100 PRs a time
        :param pull_request_cursor: the cursor that indicates where to records were loaded until
        :return: the query string
        """

        after = ''
        if pull_request_cursor:
            after = 'after:"' + pull_request_cursor + '", '

        # todo configure states dynamically.
        query = """
        {
          repository(name:\"""" + self.repository.name + """\", owner:\"""" + self.repository.owner + """\") {
            id
            pullRequests (first: 100, """ + after + """states: MERGED) { 
              totalCount 
              edges { 
                cursor 
                node { 
                  id 
                  createdAt 
                  bodyText
                  author { 
                    login 
                  } 
                  authorAssociation 
                  participants (first:1) {
                    totalCount
                  }
                  comments(first:1) {
                    totalCount
                  }
                  reviews(first:1) {
                    totalCount
                  }
                  userContentEdits(first:1) {
                    totalCount
                  }
                  reactions(first:1) {
                      totalCount
                  } 
                  commits(first: 1) {
                    totalCount
                  }
                  state
                } 
              } 
            }      
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()


class LoadParticipantsTask(repository.GitRepositoryTask, author.GitAuthorLookupMixin, repository.GitRepositoryCountMixin):
    """
    Task for loading participants from the stored pull requests
    """

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected participants for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        expected_count: int = 0
        for pull_request in pull_requests:
            expected_count += pull_request['total_participants']
        logging.debug(f'count query complete for expected participants for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual participants for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        actual_count: int = 0
        for pull_request in pull_requests:
            actual_count += len(pull_request['participants'])
        logging.debug(f'count query complete for actual participants for pull requests against {self.repository}')
        return actual_count

    def run(self):
        """
        loads the participants for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pull_request in pull_requests:
            pull_request_id: str = pull_request['id']
            participants_expected: int = pull_request['total_participants']
            participants = []
            participant_cursor: str = None
            pull_request_reviewed += 1

            if participants_expected > len(pull_request['participants']):
                while participants_expected > len(participants):
                    logging.debug(
                        f'running query for participants for pull request [{pull_request_id}] against {self.repository}'
                    )
                    query = self._pull_requests_participants_query(pull_request_id, participant_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for participants for pull request [{pull_request_id}] '
                        f'against {self.repository}'
                    )

                    # iterate over each participant returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["participants"]["edges"]:
                        participant_cursor = edge["cursor"]
                        author = self._find_author_by_id(edge["node"]["id"])
                        participants.append(author.to_dictionary())

                self._get_collection().update_one({'id': pull_request_id},
                                                  {'$set': {'participants': participants}})

            logging.debug(f'pull requests reviewed for {self.repository} {pull_request_reviewed}/{pull_request_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'participants returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    @staticmethod
    def _pull_requests_participants_query(pull_request_id: str, participant_cursor: str) -> str:
        # static method for getting the query for all the participants for a pull request

        after = ''
        if participant_cursor:
            after = ', after:"' + participant_cursor + '", '

        query = """
        {
          node(id: \"""" + pull_request_id + """\") {
            ... on PullRequest {
              participants (first:100""" + after + """) {
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


class LoadCommitIdsTask(repository.GitRepositoryTask, author.GitAuthorLookupMixin, repository.GitRepositoryCountMixin):
    """
    Task for loading commits from the stored pull requests
    """

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected commits for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        expected_count: int = 0
        for pull_request in pull_requests:
            expected_count += pull_request['total_commits']
        logging.debug(f'count query complete for expected commits for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual commits for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        actual_count: int = 0
        for pull_request in pull_requests:
            if 'commit_id_load_status' in pull_request and pull_request['commit_id_load_status'] == 'GIT_RETURNED_LESS':
                logging.error(
                    f'Pull Request {pull_request["id"]} is showing as git returned too few commits using the set '
                    f'totals to allow processing to continue'
                )
                actual_count += pull_request['total_commits']
            else:
                actual_count += len(pull_request['commit_ids'])
        logging.debug(f'count query complete for actual commits for pull requests against {self.repository}')
        return actual_count

    def run(self):
        """
        loads the commit ids for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pull_request in pull_requests:
            pull_request_id: str = pull_request['id']
            commits_expected: int = pull_request['total_commits']
            commit_ids: [str] = []
            has_next_page: bool = True
            commit_cursor: str = None
            pull_request_reviewed += 1

            if commits_expected > len(pull_request['commit_ids']):
                while has_next_page:
                    logging.debug(
                        f'running query for commits for pull request [{pull_request_id}] against {self.repository}'
                    )
                    query = self._pull_requests_commit_ids_query(pull_request_id, commit_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for commits for pull request [{pull_request_id}] against {self.repository}'
                    )

                    # get next page and cursor info
                    has_next_page = response_json["data"]["node"]["commits"]["pageInfo"]["hasNextPage"]
                    commit_cursor = response_json["data"]["node"]["commits"]["pageInfo"]["endCursor"]

                    # iterate over each participant returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["commits"]["edges"]:
                        commit_id: str = edge["node"]["commit"]["id"]
                        commit_ids.append(commit_id)

                        logging.debug(f'{len(commit_ids)}/{commits_expected} commits for '
                                      f'pull request [{pull_request_id}] against {self.repository}')

                if commits_expected == len(commit_ids):
                    self._get_collection().update_one({'id': pull_request_id},
                                                      {'$set': {'commit_ids': commit_ids,
                                                                'commit_id_load_status': 'LOADED_SUCCESSFULLY'}})
                else:
                    logging.error(
                        f'fewer commits than expected were returned from the API - expected: {commits_expected}'
                        f' actual: {len(commit_ids)}'
                    )
                    self._get_collection().update_one({'id': pull_request_id},
                                                      {'$set': {'commit_ids': commit_ids,
                                                                'commit_id_load_status': 'GIT_RETURNED_LESS'}})
            logging.debug(f'pull requests reviewed for {self.repository} {pull_request_reviewed}/{pull_request_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'commits returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    @staticmethod
    def _pull_requests_commit_ids_query(commit_id: str, commit_cursor: str) -> str:
        # static method for getting the query for all the commits for a pull request

        after = ''
        if commit_cursor:
            after = ', after:"' + commit_cursor + '", '

        query = """
        {
          node(id: \"""" + commit_id + """\") {
            ... on PullRequest {
              commits(first: 100""" + after + """) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                edges {
                  node {
                    commit{
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


class LoadEditsTask(content.GitMongoEditsTask):
    """
    Task for loading edits for stored pull requests
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the edits for a pull request

        after = ''
        if edit_cursor:
            after = 'after:"' + edit_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequest {
              id
              userContentEdits(first: 100, """ + after + """) {
                edges {
                  cursor
                  node {
                    id
                    createdAt
                    editedAt
                    editor {
                      login
                    }
                    deletedAt
                    deletedBy {
                      login
                    }
                    updatedAt
                    diff
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


class LoadReactionsTask(content.GitMongoReactionsTask):
    """
    Task for loading reactions from the stored pull requests
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the reactions for a pull request

        after = ''
        if reaction_cursor:
            after = 'after:"' + reaction_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequest {
              reactions (first:100, """ + after + """) {
                edges{
                  cursor
                  node {
                    id
                    user {
                      id
                    }
                    content
                    createdAt
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
