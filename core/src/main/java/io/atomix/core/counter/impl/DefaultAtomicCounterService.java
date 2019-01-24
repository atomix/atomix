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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Atomix long state.
 */
public class DefaultAtomicCounterService extends AbstractPrimitiveService implements AtomicCounterService {
  private final AtomicLong counter = new AtomicLong();

  public DefaultAtomicCounterService() {
    super(AtomicCounterType.instance());
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(counter.get());
  }

  @Override
  public void restore(BackupInput reader) {
    counter.set(reader.readLong());
  }

  @Override
  public void set(long value) {
    counter.set(value);
  }

  @Override
  public long get() {
    return counter.get();
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    return counter.compareAndSet(expect, update);
  }

  @Override
  public long incrementAndGet() {
    return counter.incrementAndGet();
  }

  @Override
  public long getAndIncrement() {
    return counter.getAndIncrement();
  }

  @Override
  public long decrementAndGet() {
    return counter.decrementAndGet();
  }

  @Override
  public long getAndDecrement() {
    return counter.getAndDecrement();
  }

  @Override
  public long addAndGet(long delta) {
    return counter.addAndGet(delta);
  }

  @Override
  public long getAndAdd(long delta) {
    return counter.getAndAdd(delta);
  }
}
