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
 * limitations under the License
 */
package io.atomix.protocols.raft.storage.snapshot;

import com.google.common.annotations.VisibleForTesting;
import io.atomix.utils.AtomixIOException;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a snapshot file on disk.
 */
public final class SnapshotFile {
  private static final char PART_SEPARATOR = '-';
  private static final char EXTENSION_SEPARATOR = '.';
  private static final String EXTENSION = "snapshot";
  private final File file;
  private File temporaryFile;

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable snapshot file.
   *
   * @throws NullPointerException if {@code file} is null
   */
  public static boolean isSnapshotFile(File file) {
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
    String[] parts = fileName.substring(0, fileName.lastIndexOf(EXTENSION_SEPARATOR)).split(String.valueOf(PART_SEPARATOR));

    // The total number of file name parts should be at least 2.
    if (parts.length < 2) {
      return false;
    }

    // The second part of the file name should be numeric.
    // Subtract from the number of parts to ensure PART_SEPARATOR can be used in snapshot names.
    if (!isNumeric(parts[parts.length - 1])) {
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
  static File createSnapshotFile(File directory, String serverName, long index) {
    return new File(directory, createSnapshotFileName(serverName, index));
  }

  /**
   * Creates a temporary file for writing snapshots.
   */
  static File createTemporaryFile(File directory, File base) {
    try {
      return File.createTempFile(base.getName(), null, directory);
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }
  }

  /**
   * Creates a snapshot file name from the given parameters.
   */
  @VisibleForTesting
  static String createSnapshotFileName(String serverName, long index) {
    return String.format("%s-%d.%s",
        serverName,
        index,
        EXTENSION);
  }

  /**
   * Creates a new SnapshotFile with references to a permanent snapshot file (for reading)
   * and a temporary snapshot file (for writing). The temporary file can be null if the snapshot
   * is already completed and should not be written to.
   *
   * @param file the snapshot file which is used for reading
   * @param temporaryFile the snapshot file which is used for writing
   */
  SnapshotFile(File file, File temporaryFile) {
    this.file = file;
    this.temporaryFile = temporaryFile;
  }

  /**
   * Returns the snapshot lock file name.
   *
   * @return the snapshot lock file name
   */
  File temporaryFile() {
    return temporaryFile;
  }

  void clearTemporaryFile() {
    temporaryFile = null;
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
   * Returns the snapshot name.
   *
   * @return the snapshot name
   */
  public String name() {
    return parseName(file.getName());
  }

  @VisibleForTesting
  static String parseName(String fileName) {
    return fileName.substring(0, fileName.lastIndexOf(PART_SEPARATOR, fileName.lastIndexOf(PART_SEPARATOR) - 1));
  }
}
