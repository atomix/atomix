/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.error;

/**
 * Indicates that an operation or other request from an unknown session was received.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnknownSessionException extends RaftException {
  private static final RaftError.Type TYPE = RaftError.Type.UNKNOWN_SESSION_ERROR;

  public UnknownSessionException(String message, Object... args) {
    super(TYPE, message, args);
  }

  public UnknownSessionException(Throwable cause, String message, Object... args) {
    super(TYPE, cause, message, args);
  }

  public UnknownSessionException(Throwable cause) {
    super(TYPE, cause);
  }

}
