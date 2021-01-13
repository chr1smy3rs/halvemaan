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

from halvemaan import base, repository, content, actor

luigi.auto_namespace(scope=__name__)


class PullRequest:
    """ contains the data for a pull request """

    def __init__(self):
        """
        init for pull request
        """
        self.object_type: base.ObjectType = base.ObjectType.PULL_REQUEST
        self.id: str = None
        self.repository_id: str = None
        self.author: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.author_association: str = None
        self.create_datetime: datetime = datetime.now()
        self.body_text: str = ''
        self.total_participants: int = 0
        self.participants: [actor.Actor] = []
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


class LoadPullRequestsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading pull requests from git's graphql interface
    """

    def requires(self):
        return [repository.LoadRepositoryPullRequestIdsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the pull request for a specific repository
        :return: None
        """
        if self._get_expected_results() != self._get_actual_results():
            pull_requests_loaded: int = 0

            for pull_request_id in self.repository.pull_request_ids:

                pull_requests_loaded += 1

                self._build_and_insert_pull_request(pull_request_id)

                logging.debug(
                    f'pull requests found for {self.repository} '
                    f'{pull_requests_loaded}/{self.repository.total_pull_requests}'
                )

            actual_count: int = self._get_actual_results()
            logging.debug(
                f'pull requests returned for {self.repository} returned: [{actual_count}], '
                f'expected: [{self.repository.total_pull_requests}]'
            )

    def _build_and_insert_pull_request(self, pull_request_id):
        # check to see if pull request is in the database
        found_request = self._get_collection().find_one({'id': pull_request_id,
                                                         'object_type': base.ObjectType.PULL_REQUEST.name})
        if found_request is None:
            logging.debug(f'running query for pull request: [{pull_request_id}] against {self.repository}')
            query = self._pull_request_query(pull_request_id)
            response_json = self.graph_ql_client.execute_query(query)
            logging.debug(f'query complete for pull request: [{pull_request_id}] against {self.repository}')

            pr = PullRequest()
            pr.id = response_json["data"]["node"]["id"]
            pr.repository_id = response_json["data"]["node"]["repository"]["id"]
            pr.body_text = response_json["data"]["node"]["bodyText"]
            pr.state = response_json["data"]["node"]["state"]

            # load the counts
            pr.total_reviews = response_json["data"]["node"]["reviews"]["totalCount"]
            pr.total_comments = response_json["data"]["node"]["comments"]["totalCount"]
            pr.total_participants = response_json["data"]["node"]["participants"]["totalCount"]
            pr.total_edits = response_json["data"]["node"]["userContentEdits"]["totalCount"]
            pr.total_reactions = response_json["data"]["node"]["reactions"]["totalCount"]
            pr.total_commits = response_json["data"]["node"]["commits"]["totalCount"]

            # author can be None.  Who knew?
            if response_json["data"]["node"]["author"] is not None:
                pr.author = self._find_actor_by_login(response_json["data"]["node"]["author"]["login"])
            pr.author_association = pr.author_login = response_json["data"]["node"]["authorAssociation"]

            # parse the datetime
            pr.create_datetime = base.to_datetime_from_str(response_json["data"]["node"]["createdAt"])

            logging.debug(f'inserting record for {pr}')
            self._get_collection().insert_one(pr.to_dictionary())
            logging.debug(f'insert complete for {pr}')

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        return len(self.repository.pull_request_ids)

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        return self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

    @staticmethod
    def _pull_request_query(pull_request_id: str) -> str:
        # static method for getting the query for all the fields for a pull request

        query = """
        {
          node(id: \"""" + pull_request_id + """\") {
            ... on PullRequest { 
              id 
              repository {
                id
              }
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
        """
        return query

    if __name__ == '__main__':
        luigi.run()


class LoadParticipantIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
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
                        author = self._find_actor_by_id(edge["node"]["id"])
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


class LoadReviewIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading review ids from the stored pull requests
    """

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected reviews for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        expected_count: int = 0
        for pull_request in pull_requests:
            expected_count += pull_request['total_reviews']
        logging.debug(f'count query complete for expected reviews for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual reviews for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        actual_count: int = 0
        for pull_request in pull_requests:
            actual_count += len(pull_request['review_ids'])
        logging.debug(f'count query complete for actual reviews for pull requests against {self.repository}')
        return actual_count

    def run(self):
        """
        loads the review ids for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pull_request in pull_requests:
            pull_request_id: str = pull_request['id']
            reviews_expected: int = pull_request['total_reviews']
            reviews = []
            review_cursor: str = None
            pull_request_reviewed += 1

            if reviews_expected > len(pull_request['review_ids']):
                while reviews_expected > len(reviews):
                    logging.debug(
                        f'running query for reviews for pull request [{pull_request_id}] against {self.repository}'
                    )
                    query = self._pull_requests_review_ids_query(pull_request_id, review_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for reviews for pull request [{pull_request_id}] '
                        f'against {self.repository}'
                    )

                    # iterate over each participant returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["reviews"]["edges"]:
                        review_cursor = edge["cursor"]
                        reviews.append(edge["node"]["id"])

                self._get_collection().update_one({'id': pull_request_id},
                                                  {'$set': {'review_ids': reviews}})

            logging.debug(f'pull requests reviewed for {self.repository} {pull_request_reviewed}/{pull_request_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'participants returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    @staticmethod
    def _pull_requests_review_ids_query(pull_request_id: str, review_cursor: str) -> str:
        # static method for getting the query for all the reviews for a pull request

        after = ''
        if review_cursor:
            after = ', after:"' + review_cursor + '", '

        query = """
        {
          node(id: \"""" + pull_request_id + """\") {
            ... on PullRequest {
              reviews (first:100""" + after + """) {
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


class LoadCommitIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
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


class LoadCommentIdsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading comment ids for the stored pull requests
    """

    def requires(self):
        return [LoadPullRequestsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the comment ids for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in pull_requests:
            pull_request_id: str = pr['id']
            comments_expected: int = pr['total_comments']
            comment_ids: [str] = []
            comment_cursor: str = None
            pull_request_reviewed += 1

            while comments_expected > len(comment_ids):
                logging.debug(
                    f'running query for comments for pull request [{pull_request_id}] against {self.repository}'
                )
                query = self._pull_request_comments_query(pull_request_id, comment_cursor)
                response_json = self.graph_ql_client.execute_query(query)
                logging.debug(
                    f'query complete for comments for pull request [{pull_request_id}] against {self.repository}'
                )

                # iterate over each comment returned (we return 100 at a time)
                for edge in response_json["data"]["node"]["comments"]["edges"]:

                    comment_cursor = edge["cursor"]
                    comment_ids.append(edge["node"]["id"])

            self._get_collection().update_one({'id': pull_request_id},
                                              {'$set': {'comment_ids': comment_ids}})

            logging.debug(f'pull requests reviewed for {self.repository} {pull_request_reviewed}/{pull_request_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(f'comments returned for {self.repository} '
                      f'returned: [{actual_count}], expected: [{expected_count}]')

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected comments for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        expected_count: int = 0
        for pr in pull_requests:
            expected_count += pr['total_comments']
        logging.debug(f'count query complete for expected comments for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual comments for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        actual_count: int = 0
        for pr in pull_requests:
            actual_count += len(pr['comment_ids'])
        logging.debug(f'count query complete for actual comments for pull requests against {self.repository}')
        return actual_count

    @staticmethod
    def _pull_request_comments_query(pull_request_id: str, comment_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if comment_cursor:
            after = 'after:"' + comment_cursor + '", '

        query = """
        {
          node(id: \"""" + pull_request_id + """\") {
            ... on PullRequest {
              comments(first: 100, """ + after + """) {
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


class LoadEditsTask(content.GitSingleMongoEditsTask):
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


class LoadReactionsTask(content.GitSingleMongoReactionsTask):
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
