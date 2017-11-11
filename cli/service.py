# -*- coding: utf-8

# Copyright 2017-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

from __future__ import print_function
from __future__ import unicode_literals

import requests
import json
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.lexers.textfmts import HttpLexer
from pygments.formatters import TerminalFormatter

class AtomixService(object):
    """Atomix service"""
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    DELETE = 'DELETE'

    def __init__(self, host='localhost', port=5678):
        self.host = host
        self.port = port
        self.address = 'http://%s:%s' % (self.host, self.port)
        self.last_request = None

    def url(self, path, *args, **kwargs):
        return self.address + path.format(*args, **kwargs)

    def get(self, url, headers=None, log=True):
        if log:
            self._log_request(self.GET, url, headers=headers)
        return requests.get(url, headers=headers)

    def post(self, url, data=None, headers=None, log=True):
        if log:
            self._log_request(self.POST, url, data=data, headers=headers)
        return requests.post(url, data=data, headers=headers)

    def put(self, url, data=None, headers=None, log=True):
        if log:
            self._log_request(self.PUT, url, data=data, headers=headers)
        return requests.put(url, data=data, headers=headers)

    def delete(self, url, headers=None, log=True):
        if log:
            self._log_request(self.DELETE, url, headers=headers)
        return requests.delete(url, headers=headers)

    def _log_request(self, method, url, data=None, headers=None):
        self.last_request = {
            'method': method,
            'url': url,
            'data': data,
            'headers': headers
        }

    def _print_headers(self, headers):
        if headers is not None:
            for key, value in headers.iteritems():
                print(highlight(key + ': ' + value, HttpLexer(), TerminalFormatter()))

    def _print_request(self, method, url):
        print(highlight(method + ' ' + url, HttpLexer(), TerminalFormatter()))

    def _print_data(self, data):
        if data is not None:
            if isinstance(data, dict):
                print(highlight(json.dumps(data), JsonLexer(), TerminalFormatter()))
            else:
                print(data)
