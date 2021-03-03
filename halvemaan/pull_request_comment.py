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

from halvemaan import content, base, repository, pull_request, actor

luigi.auto_namespace(scope=__name__)


class PullRequestComment(content.Comment):
    """ contains the data for a comment on a pull request """

    def __init__(self, comment_id: str):
        """
        init for a pull request comment
        :param str comment_id: the identifier for the comment
        """
        super().__init__(comment_id)
        self.pull_request_id: str = None
        self.object_type = base.ObjectType.PULL_REQUEST_COMMENT
        self.issue_id: str = ''

    def __str__(self) -> str:
        """
        returns a string identifying the comment
        :return: a string representation of the comment
        """
        return f'PullRequestComment [id: {self.id}]'

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
            'text': self.body_text,
            'minimized_status': self.minimized_status,
            'issue_id': self.issue_id,
            'total_edits': self.total_edits,
            'edits': edit_dictionaries,
            'total_reactions': self.total_reactions,
            'reactions': reaction_dictionaries,
            'is_deleted': self.is_deleted,
            'object_type': self.object_type.name
        }


class LoadCommentsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading comments for the stored pull requests
    """

    def requires(self):
        return [pull_request.LoadCommentIdsTask(owner=self.owner, name=self.name)]

    def run(self):
        """
        loads the comments for the pull requests for a specific repository
        :return: None
        """
        pull_request_reviewed: int = 0

        pull_request_count = self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST)

        pull_requests = self._get_collection().find({'repository_id': self.repository.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST.name})
        for pr in pull_requests:
            pull_request_id: str = pr['id']
            comments: [PullRequestComment] = []
            comment_ids: [str] = []
            pull_request_reviewed += 1

            for comment_id in pr['comment_ids']:
                logging.debug(
                    f'running query for comments for pull request [{pull_request_id}], '
                    f'comment id: [{comment_id}] against {self.repository}'
                )
                query = self._pull_request_comments_query(comment_id)
                response_json = self.graph_ql_client.execute_query(query)
                logging.debug(
                    f'query complete for comments for pull request [{pull_request_id}] '
                    f'comment id: [{comment_id}] against {self.repository}'
                )

                edge = response_json["data"]
                comment = PullRequestComment(edge["node"]["id"])
                comments.append(comment)
                comment_ids.append(comment.id)
                comment.repository_id = self.repository.id
                comment.pull_request_id = pull_request_id

                # get the body text
                comment.body_text = edge["node"]["bodyText"]

                # get the counts for the sub items to comment
                comment.total_reactions = edge["node"]["reactions"]["totalCount"]
                comment.total_edits = edge["node"]["userContentEdits"]["totalCount"]

                # author can be None.  Who knew?
                if edge["node"]["author"] is not None:
                    comment.author = self._find_actor_by_login(edge["node"]["author"]["login"])
                comment.author_association = edge["node"]["authorAssociation"]

                # load if the comment has been minimized
                if edge["node"]["isMinimized"] is not None and edge["node"]["isMinimized"] is True:
                    comment.minimized_status = edge["node"]["minimizedReason"]

                # load the associate issue
                if edge["node"]["issue"] is not None:
                    comment.issue_id = edge["node"]["issue"]["id"]

                # parse the datetime
                comment.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])

                # check to see if pull request comment is in the database
                found_request = \
                    self._get_collection().find_one({'id': comment.id,
                                                     'object_type': base.ObjectType.PULL_REQUEST_COMMENT.name})
                if found_request is None:
                    self._get_collection().insert_one(comment.to_dictionary())

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
            expected_count += len(pr['comment_ids'])
        logging.debug(f'count query complete for expected comments for pull requests against {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        return self._get_objects_saved_count(self.repository, base.ObjectType.PULL_REQUEST_COMMENT)

    def _get_actual_comments(self, pull_request_id: str):
        return self._get_collection().count_documents({'pull_request_id': pull_request_id,
                                                       'object_type': base.ObjectType.PULL_REQUEST_COMMENT.name})

    @staticmethod
    def _pull_request_comments_query(pull_request_comment_id: str) -> str:
        # static method for getting the query for all the comments for a pull request

        query = """
        {
          node(id: \"""" + pull_request_comment_id + """\") {
            ... on IssueComment {
              id
              createdAt
              author {
                login
              }
              authorAssociation
              userContentEdits(first: 1) {
                totalCount
              }
              isMinimized
              minimizedReason
              issue {
                id
              }
              bodyText
              reactions(first: 1) {
                totalCount
              }
            }
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()


class LoadCommentEditsTask(content.GitSingleRepositoryEditsTask):
    """
    Task for loading edits for stored pull request comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_COMMENT

    def requires(self):
        return [LoadCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if edit_cursor:
            after = 'after:"' + edit_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on IssueComment {
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


class LoadCommentReactionsTask(content.GitSingleRepositoryReactionsTask):
    """
    Task for loading reactions for stored pull request comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.PULL_REQUEST_COMMENT

    def requires(self):
        return [LoadCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the comments for a pull request

        after = ''
        if reaction_cursor:
            after = 'after:"' + reaction_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on IssueComment {
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
