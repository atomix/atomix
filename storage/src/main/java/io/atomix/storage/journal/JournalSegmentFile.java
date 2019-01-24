/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.storage.journal;

import java.io.File;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Segment file utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class JournalSegmentFile {
  private static final char PART_SEPARATOR = '-';
  private static final char EXTENSION_SEPARATOR = '.';
  private static final String EXTENSION = "log";
  private final File file;

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable segment file.
   *
   * @throws NullPointerException if {@code file} is null
   */
  public static boolean isSegmentFile(String name, File file) {
    return isSegmentFile(name, file.getName());
  }

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable segment file.
   *
   * @param journalName the name of the journal
   * @param fileName the name of the file to check
   * @throws NullPointerException if {@code file} is null
   */
  public static boolean isSegmentFile(String journalName, String fileName) {
    checkNotNull(journalName, "journalName cannot be null");
    checkNotNull(fileName, "fileName cannot be null");

    int partSeparator = fileName.lastIndexOf(PART_SEPARATOR);
    int extensionSeparator = fileName.lastIndexOf(EXTENSION_SEPARATOR);

    if (extensionSeparator == -1
        || partSeparator == -1
        || extensionSeparator < partSeparator
        || !fileName.endsWith(EXTENSION)) {
      return false;
    }

    for (int i = partSeparator + 1; i < extensionSeparator; i++) {
      if (!Character.isDigit(fileName.charAt(i))) {
        return false;
      }
    }

    return fileName.startsWith(journalName);
  }

  /**
   * Creates a segment file for the given directory, log name, segment ID, and segment version.
   */
  static File createSegmentFile(String name, File directory, long id) {
    return new File(directory, String.format("%s-%d.log", checkNotNull(name, "name cannot be null"), id));
  }

  /**
   * @throws IllegalArgumentException if {@code file} is not a valid segment file
   */
  JournalSegmentFile(File file) {
    this.file = file;
  }

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  public File file() {
    return file;
  }
}
