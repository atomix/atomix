/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.buffer;

import io.atomix.catalyst.CatalystException;

/**
 * Catalyst IO exception.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CatalystIOException extends CatalystException {

    public CatalystIOException() {
    }

    public CatalystIOException(String message) {
        super(message);
    }

    public CatalystIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalystIOException(Throwable cause) {
        super(cause);
    }

}
