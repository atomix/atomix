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
package net.kuujo.copycat.raft;

/**
 * User application exception.
 * <p>
 * Application exceptions are thrown when an exception occurs within a user-provided state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ApplicationException extends RaftException {
  private static final RaftError.Type TYPE = RaftError.Type.APPLICATION_ERROR;

  public ApplicationException(String message, Object... args) {
    super(TYPE, message, args);
  }

  public ApplicationException(Throwable cause, String message, Object... args) {
    super(TYPE, cause, message, args);
  }

  public ApplicationException(Throwable cause) {
    super(TYPE, cause);
  }

}
