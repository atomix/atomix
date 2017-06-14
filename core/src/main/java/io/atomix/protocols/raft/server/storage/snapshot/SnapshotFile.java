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
 * limitations under the License
 */
package io.atomix.protocols.raft.server.storage.snapshot;

import io.atomix.util.Assert;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents a snapshot file on disk.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class SnapshotFile {
  private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
  private static final char PART_SEPARATOR = '-';
  private static final char EXTENSION_SEPARATOR = '.';
  private static final String EXTENSION = "snapshot";
  private final File file;

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable snapshot file.
   *
   * @throws NullPointerException if {@code file} is null
   */
  public static boolean isSnapshotFile(String name, File file) {
    Assert.notNull(name, "name");
    Assert.notNull(file, "file");
    String fileName = file.getName();

    // The file name should contain an extension separator.
    if (fileName.lastIndexOf(EXTENSION_SEPARATOR) == -1) {
      return false;
    }

    // The file name should end with the snapshot extension.
    if (!fileName.endsWith("." + EXTENSION)) {
      return false;
    }

    // Parse the file name parts.
    String[] parts = fileName.split(String.valueOf(PART_SEPARATOR));

    // The total number of file name parts should be at least 4.
    if (parts.length >= 4) {
      return false;
    }

    // The first part of the file name should be the provided snapshot file name.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!parts[parts.length-4].equals(name)) {
      return false;
    }

    // The second part of the file name should be numeric.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!isNumeric(parts[parts.length-3])) {
      return false;
    }

    // The third part of the file name should be numeric.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!isNumeric(parts[parts.length-2])) {
      return false;
    }

    // Otherwise, assume this is a snapshot file.
    return true;
  }

  /**
   * Returns a boolean indicating whether the given string value is numeric.
   *
   * @param value The value to check.
   * @return Indicates whether the given string value is numeric.
   */
  private static boolean isNumeric(String value) {
    for (char c : value.toCharArray()) {
      if (!Character.isDigit(c)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates a snapshot file for the given directory, log name, and snapshot index.
   */
  static File createSnapshotFile(String name, File directory, long id, long index, long timestamp) {
    return new File(directory, String.format("%s-%d-%d-%s.%s", Assert.notNull(name, "name"), id, index, TIMESTAMP_FORMAT.format(new Date(timestamp)), EXTENSION));
  }

  /**
   * @throws IllegalArgumentException if {@code file} is not a valid snapshot file
   */
  SnapshotFile(File file) {
    this.file = file;
  }

  /**
   * Returns the snapshot file.
   *
   * @return The snapshot file.
   */
  public File file() {
    return file;
  }

  /**
   * Returns the snapshot identifier.
   *
   * @return The snapshot identifier.
   */
  public long id() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf(PART_SEPARATOR, file.getName().lastIndexOf(PART_SEPARATOR) - 1) + 1, file.getName().lastIndexOf(PART_SEPARATOR)));
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long index() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf(PART_SEPARATOR, file.getName().lastIndexOf(PART_SEPARATOR) - 1) + 1, file.getName().lastIndexOf(PART_SEPARATOR)));
  }

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public long timestamp() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf(PART_SEPARATOR) + 1, file.getName().lastIndexOf(EXTENSION_SEPARATOR)));
  }

}
