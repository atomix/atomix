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

import io.atomix.core.counter.impl.AtomicCounterOperations.AddAndGet;
import io.atomix.core.counter.impl.AtomicCounterOperations.CompareAndSet;
import io.atomix.core.counter.impl.AtomicCounterOperations.GetAndAdd;
import io.atomix.core.counter.impl.AtomicCounterOperations.Set;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import static io.atomix.core.counter.impl.AtomicCounterOperations.ADD_AND_GET;
import static io.atomix.core.counter.impl.AtomicCounterOperations.COMPARE_AND_SET;
import static io.atomix.core.counter.impl.AtomicCounterOperations.DECREMENT_AND_GET;
import static io.atomix.core.counter.impl.AtomicCounterOperations.GET;
import static io.atomix.core.counter.impl.AtomicCounterOperations.GET_AND_ADD;
import static io.atomix.core.counter.impl.AtomicCounterOperations.GET_AND_DECREMENT;
import static io.atomix.core.counter.impl.AtomicCounterOperations.GET_AND_INCREMENT;
import static io.atomix.core.counter.impl.AtomicCounterOperations.INCREMENT_AND_GET;
import static io.atomix.core.counter.impl.AtomicCounterOperations.SET;

import java.util.Objects;

/**
 * Atomix long state.
 */
public class AtomicCounterService extends AbstractPrimitiveService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicCounterOperations.NAMESPACE)
      .build());

  private Long value = 0L;

  @Override
  protected void configure(ServiceExecutor executor) {
    executor.register(SET, SERIALIZER::decode, this::set);
    executor.register(GET, this::get, SERIALIZER::encode);
    executor.register(COMPARE_AND_SET, SERIALIZER::decode, this::compareAndSet, SERIALIZER::encode);
    executor.register(INCREMENT_AND_GET, this::incrementAndGet, SERIALIZER::encode);
    executor.register(GET_AND_INCREMENT, this::getAndIncrement, SERIALIZER::encode);
    executor.register(DECREMENT_AND_GET, this::decrementAndGet, SERIALIZER::encode);
    executor.register(GET_AND_DECREMENT, this::getAndDecrement, SERIALIZER::encode);
    executor.register(ADD_AND_GET, SERIALIZER::decode, this::addAndGet, SERIALIZER::encode);
    executor.register(GET_AND_ADD, SERIALIZER::decode, this::getAndAdd, SERIALIZER::encode);
  }

  @Override
  public void backup(BufferOutput writer) {
    writer.writeLong(value);
  }

  @Override
  public void restore(BufferInput reader) {
    value = reader.readLong();
  }

  /**
   * Handles a set commit.
   *
   * @param commit the commit to handle
   */
  protected void set(Commit<Set> commit) {
    value = commit.value().value();
  }

  /**
   * Handles a get commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected Long get(Commit<Void> commit) {
    return value;
  }

  /**
   * Handles a compare and set commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected boolean compareAndSet(Commit<CompareAndSet> commit) {
    if (Objects.equals(value, commit.value().expect())) {
      value = commit.value().update();
      return true;
    }
    return false;
  }

  /**
   * Handles an increment and get commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long incrementAndGet(Commit<Void> commit) {
    Long oldValue = value;
    value = oldValue + 1;
    return value;
  }

  /**
   * Handles a get and increment commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long getAndIncrement(Commit<Void> commit) {
    Long oldValue = value;
    value = oldValue + 1;
    return oldValue;
  }

  /**
   * Handles a decrement and get commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long decrementAndGet(Commit<Void> commit) {
    Long oldValue = value;
    value = oldValue - 1;
    return value;
  }

  /**
   * Handles a get and decrement commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long getAndDecrement(Commit<Void> commit) {
    Long oldValue = value;
    value = oldValue - 1;
    return oldValue;
  }

  /**
   * Handles an add and get commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long addAndGet(Commit<AddAndGet> commit) {
    Long oldValue = value;
    value = oldValue + commit.value().delta();
    return value;
  }

  /**
   * Handles a get and add commit.
   *
   * @param commit the commit to handle
   * @return counter value
   */
  protected long getAndAdd(Commit<GetAndAdd> commit) {
    Long oldValue = value;
    value = oldValue + commit.value().delta();
    return oldValue;
  }
}