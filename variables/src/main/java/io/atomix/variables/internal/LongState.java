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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Long state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LongState extends AbstractValueState<Long> implements Snapshottable {
  private AtomicLong value = new AtomicLong(0);

  public LongState(Properties config) {
    super(config);
  }

  @Override
  public void snapshot(SnapshotWriter writer) {
    writer.writeLong(value.get());
  }

  @Override
  public void install(SnapshotReader reader) {
    value = new AtomicLong(reader.readLong());
  }

  /**
   * Handles a set commit.
   */
  @Override
  public void set(Commit<ValueCommands.Set<Long>> commit) {
    try {
      value.set(commit.operation().value());
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
      return value.get();
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
      return value.getAndSet(commit.operation().value());
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
      Long update = commit.operation().update();
      return value.compareAndSet(expect, update);
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an increment and get commit.
   */
  public long incrementAndGet(Commit<LongCommands.IncrementAndGet> commit) {
    try {
      return value.incrementAndGet();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a decrement and get commit.
   */
  public long decrementAndGet(Commit<LongCommands.DecrementAndGet> commit) {
    try {
      return value.decrementAndGet();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and increment commit.
   */
  public long getAndIncrement(Commit<LongCommands.GetAndIncrement> commit) {
    try {
      return value.getAndIncrement();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and decrement commit.
   */
  public long getAndDecrement(Commit<LongCommands.GetAndDecrement> commit) {
    try {
      return value.getAndDecrement();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add and get commit.
   */
  public long addAndGet(Commit<LongCommands.AddAndGet> commit) {
    try {
      return value.addAndGet(commit.operation().delta());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get and add commit.
   */
  public long getAndAdd(Commit<LongCommands.GetAndAdd> commit) {
    try {
      return value.getAndAdd(commit.operation().delta());
    } finally {
      commit.close();
    }
  }
}
