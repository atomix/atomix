/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.storage.snapshot;

import com.google.common.annotations.VisibleForTesting;
import io.atomix.protocols.raft.service.ServiceId;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a snapshot file on disk.
 */
public final class SnapshotFile {
  @VisibleForTesting
  static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
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
    checkNotNull(name, "name cannot be null");
    checkNotNull(file, "file cannot be null");
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
    if (parts.length < 4) {
      return false;
    }

    // The first part of the file name should be the provided snapshot file name.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!parts[parts.length - 4].equals(name)) {
      return false;
    }

    // The second part of the file name should be numeric.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!isNumeric(parts[parts.length - 3])) {
      return false;
    }

    // The third part of the file name should be numeric.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!isNumeric(parts[parts.length - 2])) {
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
  @VisibleForTesting
  static File createSnapshotFile(String name, File directory, long id, long index, long timestamp) {
    return new File(directory, createSnapshotFileName(name, id, index, timestamp));
  }

  /**
   * Creates a snapshot file name from the given parameters.
   */
  @VisibleForTesting
  static String createSnapshotFileName(String name, long id, long index, long timestamp) {
    synchronized (TIMESTAMP_FORMAT) {
      return String.format("%s-%d-%d-%s.%s",
          checkNotNull(name, "name cannot be null"),
          id,
          index,
          TIMESTAMP_FORMAT.format(new Date(timestamp)),
          EXTENSION);
    }
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
  public ServiceId snapshotId() {
    return ServiceId.from(parseId(file.getName()));
  }

  /**
   * Parses the snapshot identifier from the given file name.
   *
   * @param fileName the file name from which to parse the identifier
   * @return the identifier from the given snapshot file name
   */
  @VisibleForTesting
  static long parseId(String fileName) {
    int start = fileName.lastIndexOf(PART_SEPARATOR, fileName.lastIndexOf(PART_SEPARATOR, fileName.lastIndexOf(PART_SEPARATOR) - 1) - 1) + 1;
    int end = fileName.lastIndexOf(PART_SEPARATOR, fileName.lastIndexOf(PART_SEPARATOR) - 1);
    String idString = fileName.substring(start, end);
    return Long.parseLong(idString);
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long index() {
    return parseIndex(file.getName());
  }

  /**
   * Parses the snapshot index from the given file name.
   *
   * @param fileName the file name from which to parse the index
   * @return the index from the given snapshot file name
   */
  @VisibleForTesting
  static long parseIndex(String fileName) {
    int start = fileName.lastIndexOf(PART_SEPARATOR, fileName.lastIndexOf(PART_SEPARATOR) - 1) + 1;
    int end = fileName.lastIndexOf(PART_SEPARATOR);
    String indexString = fileName.substring(start, end);
    return Long.parseLong(indexString);
  }

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public long timestamp() {
    return parseTimestamp(file.getName());
  }

  /**
   * Parses the snapshot timestamp from the given file name.
   *
   * @param fileName the file name from which to parse the timestamp
   * @return the timestamp from the given snapshot file name
   */
  @VisibleForTesting
  static long parseTimestamp(String fileName) {
    int start = fileName.lastIndexOf(PART_SEPARATOR) + 1;
    int end = fileName.lastIndexOf(EXTENSION_SEPARATOR);
    String timestampString = fileName.substring(start, end);
    synchronized (TIMESTAMP_FORMAT) {
      try {
        Date timestamp = TIMESTAMP_FORMAT.parse(timestampString);
        return timestamp.getTime();
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
