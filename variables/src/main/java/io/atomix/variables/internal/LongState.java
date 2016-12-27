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
 * limitations under the License
 */
package io.atomix.variables.internal;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.Properties;

/**
 * Long state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LongState extends AbstractValueState<Long> implements Snapshottable {
  private Long value = 0L;

  public LongState(Properties config) {
    super(config);
  }

  @Override
  public void snapshot(SnapshotWriter writer) {
    writer.writeLong(value);
  }

  @Override
  public void install(SnapshotReader reader) {
    value = reader.readLong();
  }

  /**
   * Handles a set commit.
   */
  @Override
  public void set(Commit<ValueCommands.Set<Long>> commit) {
    try {
      Long oldValue = value;
      value = commit.operation().value();
      sendEvents(oldValue, value);
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get commit.
   */
  @Override
  public Long get(Commit<ValueCommands.Get<Long>> commit) {
    try {
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and set commit.
   */
  @Override
  public Long getAndSet(Commit<ValueCommands.GetAndSet<Long>> commit) {
    try {
      Long oldValue = value;
      value = commit.operation().value();
      sendEvents(oldValue, value);
      return oldValue;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a compare and set commit.
   */
  @Override
  public boolean compareAndSet(Commit<ValueCommands.CompareAndSet<Long>> commit) {
    try {
      Long expect = commit.operation().expect();
      if ((value == null && expect == null) || (value != null && value.equals(expect))) {
        Long oldValue = value;
        value = commit.operation().update();
        sendEvents(oldValue, value);
        return true;
      }
      return false;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an increment and get commit.
   */
  public long incrementAndGet(Commit<LongCommands.IncrementAndGet> commit) {
    try {
      Long oldValue = value;
      value = oldValue + 1;
      sendEvents(oldValue, value);
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a decrement and get commit.
   */
  public long decrementAndGet(Commit<LongCommands.DecrementAndGet> commit) {
    try {
      Long oldValue = value;
      value = oldValue - 1;
      sendEvents(oldValue, value);
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and increment commit.
   */
  public long getAndIncrement(Commit<LongCommands.GetAndIncrement> commit) {
    try {
      Long oldValue = value;
      value = oldValue + 1;
      sendEvents(oldValue, value);
      return oldValue;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and decrement commit.
   */
  public long getAndDecrement(Commit<LongCommands.GetAndDecrement> commit) {
    try {
      Long oldValue = value;
      value = oldValue - 1;
      sendEvents(oldValue, value);
      return oldValue;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add and get commit.
   */
  public long addAndGet(Commit<LongCommands.AddAndGet> commit) {
    try {
      Long oldValue = value;
      value = oldValue + commit.operation().delta();
      sendEvents(oldValue, value);
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and add commit.
   */
  public long getAndAdd(Commit<LongCommands.GetAndAdd> commit) {
    try {
      Long oldValue = value;
      value = oldValue + commit.operation().delta();
      sendEvents(oldValue, value);
      return oldValue;
    } finally {
      commit.close();
    }
  }
}
