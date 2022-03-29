// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
