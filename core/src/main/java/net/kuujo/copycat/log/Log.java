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
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Log manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Log extends Loggable {  
  /**
   * Return the log configuration.
   *
   * @return The log configuration.
   */
  LogConfig config();
  
  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append.
   * @return A list of appended entry indices.
   * @throws IllegalStateException If the log is not open.
   * @throws NullPointerException If the entries list is null.
   * @throws LogException If a new segment cannot be opened
   */
  List<Long> appendEntries(List<ByteBuffer> entries);

  /**
   * Returns the log directory.
   *
   * @return The log directory.
   */
  File directory();
  
  /**
   * Gets a list of entries from the log.
   *
   * @param from The index of the start of the list of entries to get (inclusive).
   * @param to The index of the end of the list of entries to get (inclusive).
   * @return A list of entries from the given start index to the given end index.
   * @throws IllegalStateException If the log is not open.
   */
  List<ByteBuffer> getEntries(long from, long to);

}
