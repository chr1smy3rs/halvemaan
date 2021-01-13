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

from halvemaan import repository, commit, base, actor, content

luigi.auto_namespace(scope=__name__)


class CommitComment(content.Comment):
    """ contains the data for a comment on a commit """

    def __init__(self, comment_id: str):
        """
        init for a commit comment
        :param str comment_id: identifier for the comment
        """
        super().__init__(comment_id)
        self.object_type: base.ObjectType = base.ObjectType.COMMIT_COMMENT
        self.commit_id: str = None
        self.path: str = None
        self.position: int = 0

    def __str__(self) -> str:
        """
        returns a string identifying the commit comment
        :return: a string representation of the commit comment
        """
        return f'CommitComment [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        reaction_dictionaries = list(map(base.to_dictionary, self.reactions))
        edit_dictionaries = list(map(base.to_dictionary, self.edits))

        return {
            'id': self.id,
            'commit_id': self.commit_id,
            'repository_id': self.repository_id,
            'author': self.author.to_dictionary(),
            'author_association': self.author_association,
            'create_timestamp': self.create_datetime,
            'text': self.body_text,
            'path': self.path,
            'position': self.position,
            'minimized_status': self.minimized_status,
            'total_edits': self.total_edits,
            'edits': edit_dictionaries,
            'total_reactions': self.total_reactions,
            'reactions': reaction_dictionaries,
            'is_deleted': self.is_deleted,
            'object_type': self.object_type.name
        }


class LoadCommitCommentsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin):
    """
    Task for loading comment ids for saved commits
    """

    def requires(self):
        return [commit.LoadCommitCommentIdsTask(owner=self.owner, name=self.name)]

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
            commits_reviewed += 1

            for comment_id in item['comment_ids']:

                # check to see if commit comment is in the database
                found_request = \
                    self._get_collection().find_one({'id': comment_id,
                                                     'object_type': base.ObjectType.COMMIT_COMMENT.name})
                if found_request is None:
                    logging.debug(
                        f'running query for comments for commit [{commit_id}] against {self.repository}'
                    )
                    query = self._commit_comment_query(commit_id)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for comments for commit [{commit_id}] against {self.repository}'
                    )

                    edge = response_json["data"]
                    commit_comment = CommitComment(edge["node"]["id"])
                    commit_comment.repository_id = edge["node"]["repository"]["id"]
                    commit_comment.commit_id = commit_id

                    # get the counts for the sub items to comment
                    commit_comment.total_reactions = edge["node"]["reactions"]["totalCount"]
                    commit_comment.total_edits = edge["node"]["userContentEdits"]["totalCount"]

                    # get the body text
                    commit_comment.body_text = edge["node"]["bodyText"]

                    # parse the datetime
                    commit_comment.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])

                    # set the path
                    commit_comment.path = edge["node"]["path"]

                    # author can be None.  Who knew?
                    if edge["node"]["author"] is not None:
                        commit_comment.author = self._find_actor_by_login(edge["node"]["author"]["login"])
                    commit_comment.author_association = edge["node"]['authorAssociation']

                    # set the position
                    if edge["node"]["position"] is not None:
                        commit_comment.position = edge["node"]["position"]

                    # set if the comment has been minimized
                    if edge["node"]["isMinimized"] is not None and edge["node"]["isMinimized"] is True:
                        commit_comment.minimized_status = edge["node"]["minimizedReason"]

                    self._get_collection().insert_one(commit_comment.to_dictionary())

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
            expected_count += len(item['comment_ids'])
        logging.debug(f'count query complete for expected comments for the commits in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the number of saved commit comments...
        :return: integer number
        """
        return self._get_objects_saved_count(self.repository, base.ObjectType.COMMIT_COMMENT)

    @staticmethod
    def _commit_comment_query(commit_comment_id: str) -> str:
        # static method for getting the query for a specific commit comment

        query = """
        {
          node(id: \"""" + commit_comment_id + """\") {
            ... on CommitComment {
              id
              author {
                login
              }
              authorAssociation
              repository {
                id
              }
              bodyText
              createdAt
              isMinimized
              minimizedReason
              path
              position
              reactions(first: 1) {
                totalCount
              }
              userContentEdits(first: 1) {
                totalCount
              }
            }
          }
        }
        """
        return query

    if __name__ == '__main__':
        luigi.run()


class LoadCommitCommentEditsTask(content.GitSingleMongoEditsTask):
    """
    Task for loading edits for stored commit comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.COMMIT_COMMENT

    def requires(self):
        return [LoadCommitCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the comments for a commit

        after = ''
        if edit_cursor:
            after = 'after:"' + edit_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on CommitComment {
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


class LoadCommitCommentReactionsTask(content.GitSingleMongoReactionsTask):
    """
    Task for loading reactions for stored commit comments
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type = base.ObjectType.COMMIT_COMMENT

    def requires(self):
        return [LoadCommitCommentsTask(owner=self.owner, name=self.name)]

    @staticmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the comments for a commit

        after = ''
        if reaction_cursor:
            after = 'after:"' + reaction_cursor + '", '

        query = """
        {
          node(id: \"""" + item_id + """\") {
            ... on CommitComment {
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
