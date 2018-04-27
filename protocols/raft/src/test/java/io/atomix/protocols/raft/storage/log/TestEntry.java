/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestEntry extends RaftLogEntry {
  private final byte[] bytes;

  public TestEntry(long term, int size) {
    this(term, new byte[size]);
  }

  public TestEntry(long term, byte[] bytes) {
    super(term);
    this.bytes = bytes;
  }

  public byte[] bytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("bytes", ArraySizeHashPrinter.of(bytes))
        .toString();
  }
}
