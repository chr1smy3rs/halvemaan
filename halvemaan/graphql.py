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
import json
import logging
import time

import requests


class GraphQLException(Exception):
    # base exception for this module

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class GraphQLStatusException(Exception):
    # base exception for this module

    def __init__(self, status_code):
        self.message = f'Unexpected Status Code: {status_code}'
        super().__init__(self.message)


class GraphQLClient:

    def __init__(self, url, git_token):
        self.url = url
        self.header_token = 'bearer ' + git_token

    def execute_query(self, query: str, counter: int = 3) -> json:

        try:
            response = requests.post(self.url, json={'query': query},
                                     headers={'Authorization': self.header_token})

            if response.status_code == 200:
                response_json = json.loads(response.content)
                try:
                    some_data = response_json["data"]
                    return response_json
                except KeyError:
                    logging.error(f'failed request [{response_json}]')
                    try:
                        some_data = response_json["errors"]
                        if counter != 0:
                            # todo improve this
                            logging.debug(f'failed request - guessing rate limit [{response_json}] sleeping')
                            time.sleep(1800)
                            logging.debug(f'failed request - guessing rate limit [{response_json}] sleeping complete')
                            return self.execute_query(query, counter - 1)
                        else:
                            raise GraphQLException(f'failed request - guessing rate limit [{response_json}]')
                    except KeyError:
                        if counter != 0:
                            logging.debug(f'failed request - other [{response_json}] sleeping')
                            time.sleep(60)
                            logging.debug(f'failed request - other [{response_json}] sleeping complete')
                            return self.execute_query(query, counter - 1)
                        else:
                            raise GraphQLException(f'failed request - other [{response_json}]')
            else:
                logging.error(f'failed request with status code [{response.status_code}]')
                if counter != 0:
                    logging.debug(f'failed request with status code [{response.status_code}] sleeping')
                    time.sleep(60)
                    logging.debug(f'failed request with status code [{response.status_code}] sleeping complete')
                    return self.execute_query(query, counter - 1)
                else:
                    raise GraphQLStatusException(response.status_code)

        except ConnectionError as e:
            logging.error(f'failed request with connection error')
            if counter != 0:
                logging.debug(f'failed request with connection error sleeping')
                time.sleep(60)
                logging.debug(f'failed request with connection error sleeping complete')
                return self.execute_query(query, counter - 1)
            else:
                raise e
