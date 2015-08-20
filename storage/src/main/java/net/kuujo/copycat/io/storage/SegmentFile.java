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
package net.kuujo.copycat.io.storage;

import java.io.File;

/**
 * Segment file utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class SegmentFile {
  private final File file;

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable segment file.
   */
  static boolean isSegmentFile(File file) {
    return isFile(file, "log");
  }

  private static boolean isFile(File file, String extension) {
    return file.getName().indexOf('-') != -1
      && file.getName().indexOf('-', file.getName().indexOf('-') + 1) != -1
      && file.getName().lastIndexOf('.') > file.getName().lastIndexOf('-')
      && file.getName().endsWith("." + extension);
  }

  /**
   * Creates a segment file for the given directory, log name, segment ID, and segment version.
   */
  static File createSegmentFile(File directory, long id, long version) {
    return new File(directory, String.format("copycat-%d-%d.log", id, version));
  }

  SegmentFile(File file) {
    if (!isSegmentFile(file))
      throw new IllegalArgumentException("Not a valid segment file");
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

  /**
   * Returns the segment index file.
   *
   * @return The segment index file.
   */
  public File index() {
    return new File(file.getParentFile(), file.getName().substring(0, file.getName().lastIndexOf('.') + 1) + "index");
  }

  /**
   * Returns the segment identifier.
   */
  public long id() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-', file.getName().lastIndexOf('-') - 1) + 1, file.getName().lastIndexOf('-')));
  }

  /**
   * Returns the segment version.
   */
  public long version() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-') + 1, file.getName().lastIndexOf('.')));
  }

}
