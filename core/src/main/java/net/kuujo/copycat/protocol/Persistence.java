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
package net.kuujo.copycat.protocol;

/**
 * Protocol command persistence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum Persistence {

  /**
   * Indicates a command that will not be persisted.
   */
  NONE,

  /**
   * Indicates a command that will persist until it has been replicated to all nodes.
   */
  EPHEMERAL,

  /**
   * Indicates a command that will persist until it has been filtered out of the log.
   */
  PERSISTENT,

  /**
   * Indicates a command that will be persisted according to the default persistence level.
   */
  DEFAULT

}
