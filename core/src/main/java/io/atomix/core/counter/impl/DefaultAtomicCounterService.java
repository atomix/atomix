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
package io.atomix.core.counter.impl;

import io.atomix.core.counter.AtomicCounterType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Objects;

/**
 * Atomix long state.
 */
public class DefaultAtomicCounterService extends AbstractPrimitiveService implements AtomicCounterService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .build());

  private long value;

  public DefaultAtomicCounterService() {
    super(AtomicCounterType.instance());
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(value);
  }

  @Override
  public void restore(BackupInput reader) {
    value = reader.readLong();
  }

  @Override
  public void set(long value) {
    this.value = value;
  }

  @Override
  public long get() {
    return value;
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    if (Objects.equals(value, expect)) {
      value = update;
      return true;
    }
    return false;
  }

  @Override
  public long incrementAndGet() {
    Long oldValue = value;
    value = oldValue + 1;
    return value;
  }

  @Override
  public long getAndIncrement() {
    Long oldValue = value;
    value = oldValue + 1;
    return oldValue;
  }

  @Override
  public long decrementAndGet() {
    Long oldValue = value;
    value = oldValue - 1;
    return value;
  }

  @Override
  public long getAndDecrement() {
    Long oldValue = value;
    value = oldValue - 1;
    return oldValue;
  }

  @Override
  public long addAndGet(long delta) {
    Long oldValue = value;
    value = oldValue + delta;
    return value;
  }

  @Override
  public long getAndAdd(long delta) {
    Long oldValue = value;
    value = oldValue + delta;
    return oldValue;
  }
}