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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.util.ReferencePool;

/**
 * Raft entry pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftEntryPool extends ReferencePool<RaftEntry> {

  public RaftEntryPool() {
    super(RaftEntry::new);
  }

  /**
   * Acquires an entry for the given index.
   *
   * @param index The index for which to acquire the entry.
   * @return The acquired entry.
   */
  public RaftEntry acquire(long index) {
    RaftEntry entry = super.acquire();
    entry.init(index);
    return entry;
  }

}
