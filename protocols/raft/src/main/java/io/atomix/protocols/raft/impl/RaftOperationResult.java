/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.impl;

import io.atomix.utils.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation result.
 */
public final class RaftOperationResult {
  final long index;
  final long eventIndex;
  final Object result;

  RaftOperationResult(long index, long eventIndex, Object result) {
    this.index = index;
    this.eventIndex = eventIndex;
    this.result = result;
  }

  /**
   * Returns the result index.
   *
   * @return The result index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the result event index.
   *
   * @return The result event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  /**
   * Returns the result value.
   *
   * @return The result value.
   */
  public Object result() {
    return result;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("eventIndex", eventIndex)
        .add("result", result instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) result) : result)
        .toString();
  }
}
