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
package io.atomix.variables.state;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

/**
 * Long state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LongState extends ValueState implements Snapshottable {
  private long diff;

  @Override
  public void snapshot(SnapshotWriter writer) {
    writer.writeLong(diff);
  }

  @Override
  public void install(SnapshotReader reader) {
    diff = reader.readLong();
  }

  /**
   * Handles an increment and get commit.
   */
  public long incrementAndGet(Commit<LongCommands.IncrementAndGet> commit) {
    try {
      diff++;
      Long value = (Long) this.value;
      return value != null ? value + diff : diff;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a decrement and get commit.
   */
  public long decrementAndGet(Commit<LongCommands.DecrementAndGet> commit) {
    try {
      diff--;
      return value();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and increment commit.
   */
  public long getAndIncrement(Commit<LongCommands.GetAndIncrement> commit) {
    try {
      long value = value();
      diff++;
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and decrement commit.
   */
  public long getAndDecrement(Commit<LongCommands.GetAndDecrement> commit) {
    try {
      long value = value();
      diff--;
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add and get commit.
   */
  public long addAndGet(Commit<LongCommands.AddAndGet> commit) {
    try {
      diff += commit.operation().delta();
      return value();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and add commit.
   */
  public long getAndAdd(Commit<LongCommands.GetAndAdd> commit) {
    try {
      long value = value();
      diff += commit.operation().delta();
      return value;
    } finally {
      commit.close();
    }
  }

  /**
   * Returns the current value.
   *
   * @return The current value.
   */
  private long value() {
    Long value = (Long) this.value;
    return value != null ? value + diff : diff;
  }

}
