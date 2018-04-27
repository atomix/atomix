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
 * limitations under the License.
 */
package io.atomix.primitive.service.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClockTimestamp;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Server commit.
 */
public class DefaultCommit<T> implements Commit<T> {
  private final long index;
  private final PrimitiveSession session;
  private final long timestamp;
  private final OperationId operation;
  private final T value;

  public DefaultCommit(long index, OperationId operation, T value, PrimitiveSession session, long timestamp) {
    this.index = index;
    this.session = session;
    this.timestamp = timestamp;
    this.operation = operation;
    this.value = value;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public PrimitiveSession session() {
    return session;
  }

  @Override
  public LogicalTimestamp logicalTime() {
    return LogicalTimestamp.of(index);
  }

  @Override
  public WallClockTimestamp wallClockTime() {
    return WallClockTimestamp.from(timestamp);
  }

  @Override
  public OperationId operation() {
    return operation;
  }

  @Override
  public T value() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(Commit.class, index, session.sessionId(), operation);
  }

  @Override
  public <U> Commit<U> map(Function<T, U> transcoder) {
    return new DefaultCommit<>(index, operation, transcoder.apply(value), session, timestamp);
  }

  @Override
  public Commit<Void> mapToNull() {
    return new DefaultCommit<>(index, operation, null, session, timestamp);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Commit) {
      Commit commit = (Commit) object;
      return commit.index() == index
          && commit.session().equals(session)
          && commit.operation().equals(operation)
          && Objects.equals(commit.value(), value);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("session", session)
        .add("time", wallClockTime())
        .add("operation", operation)
        .add("value", value instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) value) : value)
        .toString();
  }
}
