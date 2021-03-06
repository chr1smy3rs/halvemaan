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
from enum import Enum, auto
from functools import lru_cache

import luigi

luigi.auto_namespace(scope=__name__)


class AuthorType(Enum):
    """enum for the various author types stored within the system"""

    USER = auto()
    BOT = auto()
    MANNEQUIN = auto()
    ORGANIZATION = auto()
    ENTERPRISE_USER_ACCOUNT = auto()
    UNKNOWN = auto()


class Author:
    """class that contains the data for linking to an author, or editor or deleter"""

    def __init__(self, author_id: str, author_type: AuthorType):
        self.id: str = author_id
        self.author_type: AuthorType = author_type

    def __str__(self):
        return f'Author: [id: {self.id}] [type: {self.author_type.name}]'

    def to_dictionary(self) -> {}:
        """
        returns all of the pertinent data as a dictionary
        :return: all of the pertinent data as a dictionary
        """
        return {
            'id': self.id,
            'author_type': self.author_type.name
        }


class GitAuthorLookupMixin:
    """contains all of the lookup information for authors that don't have an identifier"""

    @lru_cache(maxsize=None)
    def _find_author_by_id(self, node_id: str) -> Author:
        logging.debug(f'running query for node: [{node_id}]')
        query = self._type_query(node_id)
        response_json = self.graph_ql_client.execute_query(query)
        logging.debug(f'query complete for user: [{node_id}]')
        try:
            typename = response_json["data"]["node"]["__typename"].upper()
            for author_type in AuthorType:
                if typename == author_type.name:
                    return Author(node_id, author_type)
            logging.error(f'could not find node type: [{node_id}][{response_json}]')
            return Author(node_id, AuthorType.UNKNOWN)

        except KeyError as e:
            logging.error(f'parsing failed for node: [{node_id}][{response_json}][{e}]')
            return Author(node_id, AuthorType.UNKNOWN)

    @lru_cache(maxsize=None)
    def _find_author_by_login(self, login: str) -> Author:
        # todo add support for bots and everything else....
        has_next_page: bool = True
        user_cursor: str = None
        while has_next_page:
            logging.debug(
                f'running query for user: [{login}]'
            )
            query = self._user_query(login, user_cursor)
            response_json = self.graph_ql_client.execute_query(query)
            logging.debug(
                f'query complete for user: [{login}]'
            )

            try:
                # get next page and cursor info
                has_next_page = response_json["data"]["search"]["pageInfo"]["hasNextPage"]
                user_cursor = response_json["data"]["search"]["pageInfo"]["endCursor"]

                # find how many users were returned
                user_jsons = response_json["data"]["search"]["nodes"]
                if len(user_jsons) == 0:

                    logging.error(f'user was not returned by the query for user: [name:{login}] '
                                  f'query: [{query}] response: [{response_json}]')

                else:
                    for user_json in user_jsons:
                        if user_json["login"] == login:
                            logging.debug(f'record found for user: [{login}: {user_json["id"]}]')
                            return Author(user_json["id"], AuthorType.USER)

            except KeyError as e:
                logging.error(f'parsing failed for user: [{login}][{response_json}][{e}]')

        logging.error(f'query complete for user: [{login}] - NO RECORD FOUND')
        return Author(login, AuthorType.UNKNOWN)

    @staticmethod
    def _type_query(node_id: str) -> str:
        # static method for getting the query for the node and type related to an id

        query = """
        {
          node(id: \"""" + node_id + """\") {
            __typename
            id
          }
        }
        """
        return query

    @staticmethod
    def _user_query(login: str, user_cursor: str) -> str:
        # static method for getting the query for all of the users related to a login

        after = ''
        if user_cursor:
            after = ', after:"' + user_cursor + '", '

        query = """
        {
          search(type: USER, query: \"""" + login + """\", first: 100""" + after + """) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on User {
                id
                login
              }
            }
          }
        }
        """
        return query