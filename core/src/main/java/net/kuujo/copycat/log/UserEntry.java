/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log;

/**
 * User log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface UserEntry extends Entry {

  /**
   * Returns the user entry index.
   *
   * @return The user entry index.
   */
  long index();

  /**
   * Returns the wrapped log entry.
   *
   * @return The wrapped log entry.
   */
  Object entry();

}
