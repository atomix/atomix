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
import java.util.Collection;

/**
 * Log manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Log extends Logger {

  /**
   * Return the log configuration.
   *
   * @return The log configuration.
   */
  LogConfig config();

  /**
   * Returns the log base file.
   *
   * @return The log base file.
   */
  File base();

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  File directory();

  /**
   * Returns a collection of log segments.
   *
   * @return A collection of log segments.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  Collection<LogSegment> segments();

  /**
   * Returns the current log segment.
   *
   * @return The current log segment.
   */
  LogSegment segment();

  /**
   * Returns the log segment for the given index.
   *
   * @param index The index for which to return the segment.
   * @return The log segment.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  LogSegment segment(long index);

  /**
   * Returns the first log segment.
   *
   * @return The first log segment.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  LogSegment firstSegment();

  /**
   * Returns the last log segment.
   *
   * @return The last log segment.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  LogSegment lastSegment();

}
