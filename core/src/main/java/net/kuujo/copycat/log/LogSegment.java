/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log;

import java.io.File;

/**
 * Log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LogSegment extends Loggable {

  /**
   * Returns the parent log.
   *
   * @return The parent log.
   */
  Log log();

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  File file();

  /**
   * Returns the segment index file.
   *
   * @return The segment index file.
   */
  File index();

  /**
   * Returns the unique segment name.
   *
   * @return The unique segment name.
   */
  long segment();

  /**
   * Returns the segment timestamp.
   *
   * @return The segment timestamp.
   */
  long timestamp();

}
