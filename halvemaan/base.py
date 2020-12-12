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
from enum import Enum, auto

import luigi
import pymongo as pymongo

from halvemaan.graphql import GraphQLClient


def to_datetime_from_str(datetime_str):
    """
    method for returning a datetime from a string
    :param datetime_str: the string to parse
    :return: the parsed datetime
    """
    return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S%z")


def to_dictionary(item) -> {}:
    """
    method for converting an item into a dictionary
    :param item: the item to convert
    :return: a dictionary of the relevant data
    """
    if item is not None:
        return item.to_dictionary()
    else:
        return {}


def to_id(item) -> str:
    """
    method for getting an id from an item
    :param item: the item to get the id from
    :return: the id fo the item
    """
    if item is not None:
        return item.id
    else:
        return ''


class HalvemaanConfig(luigi.Config):
    """ global configuration class for Halvemaan Pipeline"""
    mongo_url: str = luigi.Parameter()
    mongo_index: str = luigi.Parameter()
    mongo_collection: str = luigi.Parameter()
    github_url: str = luigi.Parameter()
    github_token: str = luigi.Parameter()


class GitMongoTask(luigi.Task, metaclass=abc.ABCMeta):
    """ base task for taking data from git and putting it into mongo """

    """ config data """
    config: HalvemaanConfig = HalvemaanConfig()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph_ql_client: GraphQLClient = GraphQLClient(self.config.github_url, self.config.github_token)
        logging.debug('connecting to the mongo database')
        self._mongo_client: pymongo.MongoClient = pymongo.MongoClient(self.config.mongo_url)
        logging.debug('connected to the mongo database')

    @abc.abstractmethod
    def requires(self):
        pass

    def output(self):
        return GitMongoTarget(self)

    def run_successful(self) -> bool:
        """
        checks that expected and actual counts are the same
        :return: True if the pull request counts are the same
        """
        for item in self.requires():
            status = item.run_successful()
            logging.debug(f'{item} - {status}')
            if not item.run_successful():
                return False

        logging.debug(f'{self}: {self._get_expected_results()} - {self._get_actual_results()}')

        if self._get_expected_results() != self._get_actual_results():
            return False

        # if we make it here, everything was successful
        return True

    @abc.abstractmethod
    def run(self):
        pass

    def _get_collection(self):
        """
        Return targeted mongo collection to query on
        """
        mongo_index = self._mongo_client[self.config.mongo_index]
        return mongo_index[self.config.mongo_collection]

    @abc.abstractmethod
    def _get_expected_results(self):
        pass

    @abc.abstractmethod
    def _get_actual_results(self):
        pass


class GitMongoTarget(luigi.Target):
    """ base task for taking data from git and putting it into mongo """

    def __init__(self, task: GitMongoTask):
        self.task: GitMongoTask = task

    def exists(self):
        return self.task.run_successful()


class ObjectType(Enum):
    """ enum for the various types of data stored from git """

    PULL_REQUEST = auto()
    PULL_REQUEST_COMMENT = auto()
    PULL_REQUEST_REVIEW = auto()
    PULL_REQUEST_REVIEW_COMMENT = auto()
    USER = auto()
    REPOSITORY = auto()
    ORGANIZATION = auto()
    COMMIT = auto()
    COMMIT_COMMENT = auto()
    CHECK_SUITE = auto()
