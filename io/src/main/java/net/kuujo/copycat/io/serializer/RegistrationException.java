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
package net.kuujo.copycat.io.serializer;

import net.kuujo.copycat.CopycatException;

/**
 * Registration exception.
 * <p>
 * Registration exceptions are thrown by the {@link SerializerRegistry} when an attempt is made to
 * register a serializable type that has already been registered. This ensures that only the first instance of any
 * serializable type can be registered, and subsequent attempts to register serializers for the same type can be suppressed
 * by catching this exception.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegistrationException extends CopycatException {
  public RegistrationException() {
  }

  public RegistrationException(String message) {
    super(message);
  }

  public RegistrationException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistrationException(Throwable cause) {
    super(cause);
  }
}
