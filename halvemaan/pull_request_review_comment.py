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

import luigi

from halvemaan import content, base, repository, pull_request_review, actor

luigi.auto_namespace(scope=__name__)


class PullRequestReviewComment(content.Comment):
    """ contains the data for a comment on a pull request review """

    def __init__(self, comment_id: str):
        """
        init for a review comment
        :param str comment_id: identifier for the comment
        """
        super().__init__(comment_id)
        self.object_type: base.ObjectType = base.ObjectType.PULL_REQUEST_REVIEW_COMMENT
        self.pull_request_id: str = None
        self.pull_request_review_id: str = None
        self.original_commit_id: str = None
        self.commit_id: str = None
        self.reply_to_comment_id: str = None
        self.path: str = None
        self.original_position: int = 0
        self.position: int = 0
        self.diff_hunk: str = None
        self.state: str = None

    def __str__(self) -> str:
        """
        returns a string identifying the review comment
        :return: a string representation of the review comment
        """
        return f'PullRequestReviewComment [id: {self.id}]'

    def inline_comment(self) -> bool:
        """
        returns whether or not this is an inline comment
        :return: True if the position of the code was set
        """
        return self.original_position != 0

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
            'pull_request_review_id': self.pull_request_review_id,
            'repository_id': self.repository_id,
            'author': self.author.to_dictionary(),
            'author_association': self.author_association,
            'original_commit_id': self.original_commit_id,
            'commit_id': self.commit_id,
            'reply_to_comment_id': self.reply_to_comment_id,
            'create_timestamp': self.create_datetime,
            'text': self.body_text,
            'inline_comment': self.inline_comment(),
            'diff_hunk': self.diff_hunk,
            'path': self.path,
            'original_position': self.original_position,
            'position': self.position,
            'minimized_status': self.minimized_status,
            'total_edits': self.total_edits,
            'edits': edit_dictionaries,
            'total_reactions': self.total_reactions,
            'reactions': reaction_dictionaries,
            'is_deleted': self.is_deleted,
            'state': self.state,
            'object_type': self.object_type.name
        }


class LoadReviewCommentsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading comments for the stored pull request reviews
    """

    def requires(self):
        return [pull_request_review.LoadReviewCommentIdsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the comments for the pull request reviews for a specific repository
        :return: None
        """
        pull_request_reviews_reviewed: int = 0

        pull_request_review_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST_REVIEW)

        pull_request_reviews = self._get_collection().find({'repository_id': self.repository.id,
                                                           'object_type': base.ObjectType.PULL_REQUEST_REVIEW.name})
        for review in pull_request_reviews:
            pull_request_reviews_reviewed += 1

            for comment_id in review['comment_ids']:
                self._build_and_insert_pull_request_review_comment(comment_id)

            logging.debug(f'pull request reviews reviewed for {self.repository} '
                          f'{pull_request_reviews_reviewed}/{pull_request_review_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(f'comments returned for {self.repository} '
                      f'returned: [{actual_count}], expected: [{expected_count}]')

    def _build_and_insert_pull_request_review_comment(self, comment_id: str):

        # check to see if pull request comment is in the database
        found_request = \
            self._get_collection().find_one({'id': comment_id, 'object_type':
                                            base.ObjectType.PULL_REQUEST_REVIEW_COMMENT.name})
        if found_request is None:

            logging.debug(f'running query for comment [{comment_id}] against {self.repository}')
            query = self._pull_request_review_comments_query(comment_id)
            response_json = self.graph_ql_client.execute_query(query)
            logging.debug(f'query complete for comment [{comment_id}] against {self.repository}')

            edge = response_json["data"]
            comment = PullRequestReviewComment(edge["node"]["id"])
            comment.repository_id = edge["node"]["repository"]["id"]
            comment.pull_request_id = edge["node"]["pullRequestReview"]["id"]
            comment.pull_request_review_id = edge["node"]["pullRequestReview"]["pullRequest"]["id"]
            comment.state = edge["node"]["state"]

            # get the body text
            comment.body_text = edge["node"]["bodyText"]

            # get the counts for the sub items to comment
            comment.total_reactions = edge["node"]["reactions"]["totalCount"]
            comment.total_edits = edge["node"]["userContentEdits"]["totalCount"]

            # parse the datetime
            comment.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])

            # set the diffHunk
            comment.diff_hunk = edge["node"]["diffHunk"]

            # set the path
            comment.path = edge["node"]["path"]

            # set the original position
            if edge["node"]["originalPosition"] is not None:
                comment.original_position = edge["node"]["originalPosition"]

            # set the position
            if edge["node"]["position"] is not None:
                comment.position = edge["node"]["position"]

            # set if the comment has been minimized
            if edge["node"]["isMinimized"] is not None and edge["node"]["isMinimized"] is True:
                comment.minimized_status = edge["node"]["minimizedReason"]

            # author can be None.  Who knew?
            if edge["node"]["author"] is not None:
                comment.author = self._find_actor_by_login(edge["node"]["author"]["login"])
            comment.author_association = edge["node"]['authorAssociation']

            # get original commit id
            if edge["node"]["originalCommit"] is not None:
                comment.original_commit_id = edge["node"]["originalCommit"]["id"]

            # get commit id
            if edge["node"]["commit"] is not None:
                comment.commit_id = edge["node"]["commit"]["id"]

            # get author of comment we are replying to
            if edge["node"]["replyTo"] is not None:
                comment.reply_to_comment_id = edge["node"]["replyTo"]["id"]

            self._get_collection().insert_one(comment.to_dictionary())

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected review comments for pull requests against {self.repository}')
        pull_request_reviews = self._get_collection().find({'repository_id': self.repository.id,
                                                           'object_type': 'PULL_REQUEST_REVIEW'})
        expected_count: int = 0
        for review in pull_request_reviews:
            expected_count += len(review['comment_ids'])
        logging.debug(f'count query complete for expected review comments for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        return self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST_REVIEW_COMMENT)

    def _get_actual_comments(self, pull_request_review_id: str):
        return self._get_collection().count_documents({'pull_request_review_id': pull_request_review_id,
                                                       'object_type': base.ObjectType.PULL_REQUEST_REVIEW_COMMENT.name})

    @staticmethod
    def _pull_request_review_comments_query(pull_request_review_comment_id: str) -> str:
        # static method for getting the query for all the comments for a pull request review

        query = """
        {
          node(id: \"""" + pull_request_review_comment_id + """\") {
            ... on PullRequestReviewComment {
              id
              pullRequestReview {
                id
                pullRequest {
                  id
                }
              }
              author {
                login
              }
              authorAssociation
              repository {
                id
              }
              bodyText
              createdAt
              diffHunk
              path
              originalPosition
              position
              isMinimized
              minimizedReason
              originalCommit {
                id
              }
              commit {
                id
              }
              replyTo {
                id
              }
              userContentEdits(first: 1) {
                totalCount
              }
              reactions(first: 1) {
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


class LoadReviewCommentEditsTask(content.GitSingleMongoEditsTask):
    """
    Task for loading edits for stored pull request review comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW_COMMENT

    def requires(self):
        return [LoadReviewCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if edit_cursor:
            after = 'after:"' + edit_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequestReviewComment {
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


class LoadReviewCommentReactionsTask(content.GitSingleMongoReactionsTask):
    """
    Task for loading reactions for stored pull request review comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_REVIEW_COMMENT

    def requires(self):
        return [LoadReviewCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if reaction_cursor:
            after = 'after:"' + reaction_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on PullRequestReviewComment {
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
