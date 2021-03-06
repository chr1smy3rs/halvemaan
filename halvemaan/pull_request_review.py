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

from halvemaan import base, user, content, repository, pull_request, author

luigi.auto_namespace(scope=__name__)


class PullRequestReview:
    """ contains the data for a review on a pull request """

    def __init__(self, review_id: str, pull_request_id: str):
        """
        init for a review of a pull request
        :param str review_id: the identifier for the pull request
        :param PullRequest pr: the pull request this review pertains to
        """
        self.object_type: base.ObjectType = base.ObjectType.PULL_REQUEST_REVIEW
        self.id: str = review_id
        self.pull_request_id: str = pull_request_id
        self.repository_id: str = None
        self.author: author.Author = author.Author('', author.AuthorType.UNKNOWN)
        self.author_association: str = ''
        self.create_datetime: datetime = datetime.now()
        self.commit_id: str = None
        self.body_text: str = ''
        self.total_edits: int = 0
        self.edits: [content.ContentEdit] = []
        self.total_reactions: int = 0
        self.reactions: [content.Reaction] = []
        self.total_comments: int = 0
        self.comment_ids: [str] = []
        # mapped to onBehalfOf
        self.total_for_teams: int = 0
        self.for_team_ids: [str] = []
        self.state: str = None

    def __str__(self) -> str:
        """
        returns a string identifying the review
        :return: a string representation of the review
        """
        return f'PullRequestReview [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        reaction_dictionaries = list(map(base.to_dictionary, self.reactions))
        edit_dictionaries = list(map(base.to_dictionary, self.edits))

        return {
            'id': self.id,
            'pull_request_id': self.pull_request_id,
            'repository_id': self.repository_id,
            'author': self.author.to_dictionary(),
            'author_association': self.author_association,
            'create_timestamp': self.create_datetime,
            'commit_id': self.commit_id,
            'text': self.body_text,
            'total_comments': self.total_comments,
            'comment_ids': self.comment_ids,
            'total_for_teams': self.total_for_teams,
            'for_team_ids': self.for_team_ids,
            'total_reactions': self.total_reactions,
            'reactions': reaction_dictionaries,
            'total_edits': self.total_edits,
            'edits': edit_dictionaries,
            'state': self.state,
            'object_type': self.object_type.name
        }


class LoadReviewsTask(repository.GitRepositoryTask, author.GitAuthorLookupMixin, repository.GitRepositoryCountMixin):
    """
    Task for loading reviews for the stored pull requests
    """

    def requires(self):
        return [pull_request.LoadPullRequestsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the reviews for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in pull_requests:
            pull_request_id: str = pr['id']
            reviews_expected: int = pr['total_reviews']
            review_ids: [str] = []
            review_cursor: str = None
            pull_request_reviewed += 1

            while reviews_expected > self._get_actual_reviews(pull_request_id):
                logging.debug(
                    f'running query for reviews for pull request [{pull_request_id}] against {self.repository}'
                )
                query = self._pull_request_reviews_query(pull_request_id, review_cursor)
                response_json = self.graph_ql_client.execute_query(query)
                logging.debug(
                    f'query complete for reviews for pull request [{pull_request_id}] against {self.repository}'
                )

                # iterate over each review returned (we return 100 at a time)
                for edge in response_json["data"]["node"]["reviews"]["edges"]:

                    review_cursor = edge["cursor"]
                    review = PullRequestReview(edge["node"]["id"], pull_request_id)
                    review_ids.append(review.id)
                    review.repository_id = self.repository.id
                    review.body_text = edge["node"]["bodyText"]
                    review.commit_id = edge["node"]["commit"]["id"]
                    review.total_comments = edge["node"]["comments"]["totalCount"]
                    review.total_edits = edge["node"]["userContentEdits"]["totalCount"]
                    review.total_reactions = edge["node"]["reactions"]["totalCount"]
                    review.total_for_teams = edge["node"]["onBehalfOf"]["totalCount"]
                    review.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])
                    review.state = edge["node"]["state"]

                    # author can be None.  Who knew?
                    if edge["node"]["author"] is not None:
                        review.author = self._find_author_by_login(edge["node"]["author"]["login"])
                    review.author_association = edge["node"]["authorAssociation"]

                    # check to see if pull request review is in the database
                    found_request = self._get_collection().find_one({'id': review.id,
                                                                    'object_type': 'PULL_REQUEST_REVIEW'})
                    if found_request is None:
                        self._get_collection().insert_one(review.to_dictionary())

                self._get_collection().update_one({'id': pull_request_id},
                                                  {'$set': {'review_ids': review_ids}})

            logging.debug(f'pull requests reviewed for {self.repository} {pull_request_reviewed}/{pull_request_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(f'reviews returned for {self.repository} '
                      f'returned: [{actual_count}], expected: [{expected_count}]')

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected reviews for pull requests against {self.repository}')
        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        expected_count: int = 0
        for pr in pull_requests:
            expected_count += pr['total_reviews']
        logging.debug(f'count query complete for expected reviews for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        return self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST_REVIEW)

    def _get_actual_reviews(self, pull_request_id: str):
        return self._get_collection().count({'pull_request_id': pull_request_id,
                                             'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})

    @staticmethod
    def _pull_request_reviews_query(pull_request_id: str, review_cursor: str) -> str:
        # static method for getting the query for all the reviews for a pull request

        after = ''
        if review_cursor:
            after = 'after:"' + review_cursor + '", '

        query = """
        {
          node(id: \"""" + pull_request_id + """\") {
            ... on PullRequest {
              reviews(first: 100, """ + after + """) {
                edges {
                  cursor
                  node {
                    id
                    author {
                      login
                    } 
                    authorAssociation 
                    bodyText
                    createdAt
                    commit {
                      id
                    }
                    comments(first:1) {
                      totalCount
                    }
                    reactions(first: 1){
                      totalCount
                    }
                    userContentEdits(first:1){
                      totalCount
                    }
                    onBehalfOf(first: 1) {
                      totalCount
                    }            
                    state
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


class LoadReviewEditsTask(content.GitMongoEditsTask):
    """
    Task for loading edits for stored pull request reviews
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW

    def requires(self):
        return [LoadReviewsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if edit_cursor:
            after = 'after:"' + edit_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequestReview {
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


class LoadReviewReactionsTask(content.GitMongoReactionsTask):
    """
    Task for loading reactions for stored pull request reviews
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW

    def requires(self):
        return [LoadReviewsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if reaction_cursor:
            after = 'after:"' + reaction_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequestReview {
              id
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
