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
package io.atomix.core.map.impl;

import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;

import java.util.HashMap;
import java.util.Map;

/**
 * Atomic counter map state for Atomix.
 * <p>
 * The counter map state is implemented as a snapshottable state machine. Snapshots are necessary
 * since incremental compaction is impractical for counters where the value of a counter is the sum
 * of all its increments. Note that this snapshotting large state machines may risk blocking of the
 * Raft cluster with the current implementation of snapshotting in Copycat.
 */
public class DefaultAtomicCounterMapService extends AbstractPrimitiveService implements AtomicCounterMapService {
  private Map<String, Long> map = new HashMap<>();

  public DefaultAtomicCounterMapService() {
    super(AtomicCounterMapType.instance());
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeObject(map);
  }

  @Override
  public void restore(BackupInput reader) {
    map = reader.readObject();
  }

  /**
   * Returns the primitive value for the given primitive wrapper.
   */
  private long primitive(Long value) {
    if (value != null) {
      return value;
    } else {
      return 0;
    }
  }

  @Override
  public long put(String key, long value) {
    return primitive(map.put(key, value));
  }

  @Override
  public long putIfAbsent(String key, long value) {
    return primitive(map.putIfAbsent(key, value));
  }

  @Override
  public long get(String key) {
    return primitive(map.get(key));
  }

  @Override
  public boolean replace(String key, long expectedOldValue, long newValue) {
    Long value = map.get(key);
    if (value == null) {
      if (expectedOldValue == 0) {
        map.put(key, newValue);
        return true;
      } else {
        return false;
      }
    } else if (value == expectedOldValue) {
      map.put(key, newValue);
      return true;
    }
    return false;
  }

  @Override
  public long remove(String key) {
    return primitive(map.remove(key));
  }

  @Override
  public boolean remove(String key, long value) {
    Long oldValue = map.get(key);
    if (oldValue == null) {
      if (value == 0) {
        map.remove(key);
        return true;
      }
      return false;
    } else if (oldValue == value) {
      map.remove(key);
      return true;
    }
    return false;
  }

  @Override
  public long getAndIncrement(String key) {
    long value = primitive(map.get(key));
    map.put(key, value + 1);
    return value;
  }

  @Override
  public long getAndDecrement(String key) {
    long value = primitive(map.get(key));
    map.put(key, value - 1);
    return value;
  }

  @Override
  public long incrementAndGet(String key) {
    long value = primitive(map.get(key));
    map.put(key, ++value);
    return value;
  }

  @Override
  public long decrementAndGet(String key) {
    long value = primitive(map.get(key));
    map.put(key, --value);
    return value;
  }

  @Override
  public long addAndGet(String key, long delta) {
    long value = primitive(map.get(key));
    value += delta;
    map.put(key, value);
    return value;
  }

  @Override
  public long getAndAdd(String key, long delta) {
    long value = primitive(map.get(key));
    map.put(key, value + delta);
    return value;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public void clear() {
    map.clear();
  }
}
