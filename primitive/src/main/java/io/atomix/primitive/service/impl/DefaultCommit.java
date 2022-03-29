// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.service.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.session.Session;
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
  private final Session session;
  private final long timestamp;
  private final OperationId operation;
  private final T value;

  public DefaultCommit(long index, OperationId operation, T value, Session session, long timestamp) {
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
  public Session session() {
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
    if (object == this) {
      return true;
    }
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
