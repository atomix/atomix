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
package net.kuujo.copycat.log.impl;

/**
 * Entry indicating the end of a snapshot entry set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotEndEntry extends SnapshotEntry {
  private static final long serialVersionUID = -7343120000478427483L;
  private long length;

  public SnapshotEndEntry() {
    super();
  }

  public SnapshotEndEntry(long term, long length) {
    super(term);
    this.length = length;
  }

  /**
   * Returns the total length of the snapshot.
   *
   * @return The total byte length of the snapshot.
   */
  public long length() {
    return length;
  }

}
