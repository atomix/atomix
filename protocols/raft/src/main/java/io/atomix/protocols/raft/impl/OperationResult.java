/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation result.
 */
public final class OperationResult {

  /**
   * Returns a no-op operation result.
   *
   * @param index the result index
   * @param eventIndex the session's last event index
   * @return the operation result
   */
  public static OperationResult noop(long index, long eventIndex) {
    return new OperationResult(index, eventIndex, null, null);
  }

  /**
   * Returns a successful operation result.
   *
   * @param index the result index
   * @param eventIndex the session's last event index
   * @param result the operation result value
   * @return the operation result
   */
  public static OperationResult succeeded(long index, long eventIndex, byte[] result) {
    return new OperationResult(index, eventIndex, null, result);
  }

  /**
   * Returns a failed operation result.
   *
   * @param index the result index
   * @param eventIndex the session's last event index
   * @param error the operation error
   * @return the operation result
   */
  public static OperationResult failed(long index, long eventIndex, Throwable error) {
    return new OperationResult(index, eventIndex, error, null);
  }

  private final long index;
  private final long eventIndex;
  private final Throwable error;
  private final byte[] result;

  private OperationResult(long index, long eventIndex, Throwable error, byte[] result) {
    this.index = index;
    this.eventIndex = eventIndex;
    this.error = error;
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
   * Returns the operation error.
   *
   * @return the operation error
   */
  public Throwable error() {
    return error;
  }

  /**
   * Returns the result value.
   *
   * @return The result value.
   */
  public byte[] result() {
    return result;
  }

  /**
   * Returns whether the operation succeeded.
   *
   * @return whether the operation succeeded
   */
  public boolean succeeded() {
    return error == null;
  }

  /**
   * Returns whether the operation failed.
   *
   * @return whether the operation failed
   */
  public boolean failed() {
    return !succeeded();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("eventIndex", eventIndex)
        .add("error", error)
        .add("result", ArraySizeHashPrinter.of(result))
        .toString();
  }
}
