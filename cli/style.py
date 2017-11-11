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

from pygments.token import Token
from pygments.util import ClassNotFound
from prompt_toolkit.styles import default_style_extensions, style_from_dict
from pygments.styles import get_style_by_name

def get_style(theme):
    try:
        style = get_style_by_name(theme)
    except ClassNotFound:
        style = get_style_by_name('native')

        # Create styles dictionary.
    styles = {}
    styles.update(style.styles)
    styles.update(default_style_extensions)
    styles.update({
        Token.Menu.Completions.Completion.Current: 'bg:#00aaaa #000000',
        Token.Menu.Completions.Completion: 'bg:#008888 #ffffff',
        Token.Scrollbar: 'bg:#00aaaa',
        Token.Scrollbar.Button: 'bg:#003333',
        Token.Toolbar: 'bg:#222222 #cccccc',
        Token.Toolbar.Off: 'bg:#222222 #696969',
        Token.Toolbar.On: 'bg:#222222 #ffffff',
        Token.Toolbar.Search: 'noinherit bold',
        Token.Toolbar.Search.Text: 'nobold',
        Token.Toolbar.System: 'noinherit bold',
        Token.Toolbar.Arg: 'noinherit bold',
        Token.Toolbar.Arg.Text: 'nobold',
        Token.Method: 'bg:#222222 #dddddd',
        Token.Url: 'bg:#222222 #dddddd noinherit italic',
        Token.Headers: 'bg:#222222 #999999'
    })
    return style_from_dict(styles)
