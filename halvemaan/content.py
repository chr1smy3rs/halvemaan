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

from halvemaan import repository, base, actor


class ContentEdit:
    """ contains the data around an edit of content (a comment) """

    def __init__(self, edit_id: str):
        """
        init for an edit
        :param str edit_id: the identifier for the edit
        """
        self.id: str = edit_id
        self.edit_datetime: datetime = datetime.now()
        self.editor: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.difference: str = ''
        self.is_delete: bool = False

    def __str__(self) -> str:
        """
        returns a string identifying the edit
        :return: a string representation of the edit
        """
        return f'ContentEdit [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'editor': self.editor.to_dictionary(),
            'edited_timestamp': self.edit_datetime,
            'difference': self.difference
        }


class GitSingleRepositoryEditsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin,
                                   metaclass=abc.ABCMeta):
    """
    base task for loading edits for stored documents
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type: base.ObjectType = None

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected edits for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        expected_count: int = 0
        for item in items:
            expected_count += item['total_edits']
        logging.debug(f'count query complete for expected edits for {self.object_type.name} in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual edits for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        expected_count: int = 0
        for item in items:
            expected_count += len(item['edits'])
        logging.debug(f'count query complete for actual edits for {self.object_type.name} in {self.repository}')
        return expected_count

    def run(self):
        """
        loads the edits for a specific object type for a specific repository
        :return: None
        """
        item_reviewed: int = 0

        item_count = self._get_objects_saved_count(self.repository, self.object_type)

        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        for item in items:
            item_reviewed += 1
            item_id: str = item['id']
            edits_expected: int = item['total_edits']
            edits: [ContentEdit] = []
            edit_cursor: str = None
            has_delete: bool = False

            if edits_expected > len(item['edits']):
                while edits_expected > len(edits):
                    logging.debug(f'running query for edits for {self.object_type.name} [{item_id}] '
                                  f'against {self.repository}')
                    query = self._edits_query(item_id, edit_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(f'query complete for edits for {self.object_type.name} [{item_id}] '
                                  f'against {self.repository}')

                    # iterate over each edit returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["userContentEdits"]["edges"]:
                        edit_cursor = edge["cursor"]
                        edit = ContentEdit(edge["node"]["id"])
                        edits.append(edit)
                        edit.edit_datetime = base.to_datetime_from_str(edge["node"]["editedAt"])
                        if edge["node"]["editor"] is not None:
                            edit.editor = self._find_actor_by_login(edge["node"]["editor"]["login"])
                        # checking to see if this is an edit
                        if edge["node"]["diff"] is not None:
                            edit.difference = edge["node"]["diff"]

                        if edge["node"]["deletedAt"] is not None:
                            if edge["node"]["deletedBy"] is not None:
                                edit.editor = self._find_actor_by_login(edge["node"]["deletedBy"]["login"])
                            edit.edit_datetime = base.to_datetime_from_str(edge["node"]["deletedAt"])
                            edit.is_delete = True
                            has_delete = True

                edit_dictionaries = list(map(base.to_dictionary, edits))
                self._get_collection().update_one({'id': item_id},
                                                  {'$set': {'edits': edit_dictionaries, 'is_deleted': has_delete}})
            logging.debug(
                f'{self.object_type.name} reviewed for {self.repository} {item_reviewed}/{item_count}'
            )

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(f'edits returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]')

    @staticmethod
    @abc.abstractmethod
    def _edits_query(item_id: str, edit_cursor: str) -> str:
        # static method for getting the query for all the edits for a pull request
        pass


class Reaction:
    """ contains the data for a reaction for a comment within a pull request, or a review """

    def __init__(self, reaction_id: str):
        """
        init for the reaction
        :param str reaction_id: identifier for the reaction
        """
        self.id: str = reaction_id
        self.author: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.create_datetime: datetime = datetime.now()
        self.content: str = ''

    def __str__(self) -> str:
        """
        returns a string identifying the reaction
        :return: a string representation of the reaction
        """
        return f'Reaction [id: {self.id}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'author': self.author.to_dictionary(),
            'create_timestamp': self.create_datetime,
            'content': self.content
        }


class GitSingleRepositoryReactionsTask(repository.GitSingleRepositoryTask, actor.GitActorLookupMixin,
                                       metaclass=abc.ABCMeta):
    """
    base task for loading reactions for stored documents
    """

    def __init__(self, *args, **kwargs):
        """
            sets up the object type for query
        """
        super().__init__(*args, **kwargs)
        self.object_type: base.ObjectType = None

    def _get_expected_results(self):
        """
        returns the expected count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for expected reactions for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        expected_count: int = 0
        for item in items:
            expected_count += item['total_reactions']
        logging.debug(f'count query complete for expected reactions for {self.object_type.name} in {self.repository}')
        return expected_count

    def _get_actual_results(self):
        """
        returns the actual count per repository
        :return: expected counts
        """
        logging.debug(f'running count query for actual reactions for {self.object_type.name} in {self.repository}')
        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        actual_count: int = 0
        for item in items:
            actual_count += len(item['reactions'])
        logging.debug(f'count query complete for actual reactions for {self.object_type.name} in {self.repository}')
        return actual_count

    def run(self):
        """
        loads the reactions for a document type for a specific repository
        :return: None
        """
        items_reviewed: int = 0

        item_count = self._get_objects_saved_count(self.repository, self.object_type)

        items = self._get_collection().find({'repository_id': self.repository.id, 'object_type': self.object_type.name})
        for item in items:
            item_id: str = item['id']
            reactions_expected: int = item['total_reactions']
            reactions: [Reaction] = []
            reaction_cursor: str = None
            items_reviewed += 1

            if reactions_expected > len(item['reactions']):
                while reactions_expected > len(reactions):
                    logging.debug(
                        f'running query for reactions for {self.object_type.name} [{item_id}] against {self.repository}'
                    )
                    query = self._reactions_query(item_id, reaction_cursor)
                    response_json = self.graph_ql_client.execute_query(query)
                    logging.debug(
                        f'query complete for reactions for {self.object_type.name} [{item_id}] '
                        f'against {self.repository}'
                    )

                    # iterate over each reaction returned (we return 100 at a time)
                    for edge in response_json["data"]["node"]["reactions"]["edges"]:
                        reaction_cursor = edge["cursor"]
                        reaction = Reaction(edge["node"]["id"])
                        reaction.content = edge["node"]["content"]
                        reaction.create_datetime = base.to_datetime_from_str(edge["node"]["createdAt"])

                        # author can be None.  Who knew?
                        if edge["node"]["user"] is not None:
                            reaction.author = self._find_actor_by_id(edge["node"]["user"]["id"])
                        reactions.append(reaction)

                reaction_dictionaries = list(map(base.to_dictionary, reactions))
                self._get_collection().update_one({'id': item_id},
                                                  {'$set': {'reactions': reaction_dictionaries}})

            logging.debug(f'{self.object_type.name} reviewed for {self.repository} {items_reviewed}/{item_count}')

        actual_count: int = self._get_actual_results()
        expected_count: int = self._get_expected_results()
        logging.debug(
            f'reactions returned for {self.repository} returned: [{actual_count}], expected: [{expected_count}]'
        )

    @staticmethod
    @abc.abstractmethod
    def _reactions_query(item_id: str, reaction_cursor: str) -> str:
        # static method for getting the query for all the reactions for a pull request
        pass


class Comment:
    """ base class for a comment within a pull request or review """

    def __init__(self, comment_id: str):
        """
        init for the base comment
        :param str comment_id: the identifier for the comment
        """
        self.id: str = comment_id
        self.repository_id: str = None
        self.author: actor.Actor = actor.Actor('', actor.ActorType.UNKNOWN)
        self.author_association: str = ''
        self.create_datetime: datetime = datetime.now()
        self.body_text: str = ''
        self.total_reactions: int = 0
        self.reactions: [Reaction] = []
        self.minimized_status: str = 'NOT MINIMIZED'
        self.total_edits: int = 0
        self.edits: [ContentEdit] = []
        self.is_deleted: bool = False