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

import java.util.Set;

/**
 * Entry indicating the start of a snapshot set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotStartEntry extends SnapshotEntry {
  private static final long serialVersionUID = 2026796603990813501L;
  private Set<String> cluster;

  public SnapshotStartEntry() {
    super();
  }

  public SnapshotStartEntry(long term, Set<String> cluster) {
    super(term);
    this.cluster = cluster;
  }

  /**
   * Returns the snapshot cluster configuration.
   *
   * @return The snapshot cluster configuration.
   */
  public Set<String> cluster() {
    return cluster;
  }

}
