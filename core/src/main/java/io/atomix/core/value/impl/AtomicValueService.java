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
package io.atomix.core.value.impl;

import com.google.common.collect.Sets;

import io.atomix.core.value.AtomicValueEvent;
import io.atomix.core.value.impl.AtomicValueOperations.CompareAndSet;
import io.atomix.core.value.impl.AtomicValueOperations.GetAndSet;
import io.atomix.core.value.impl.AtomicValueOperations.Set;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import static io.atomix.core.value.impl.AtomicValueEvents.CHANGE;
import static io.atomix.core.value.impl.AtomicValueOperations.ADD_LISTENER;
import static io.atomix.core.value.impl.AtomicValueOperations.COMPARE_AND_SET;
import static io.atomix.core.value.impl.AtomicValueOperations.GET;
import static io.atomix.core.value.impl.AtomicValueOperations.GET_AND_SET;
import static io.atomix.core.value.impl.AtomicValueOperations.REMOVE_LISTENER;
import static io.atomix.core.value.impl.AtomicValueOperations.SET;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Raft atomic value service.
 */
public class AtomicValueService extends AbstractPrimitiveService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicValueOperations.NAMESPACE)
      .register(AtomicValueEvents.NAMESPACE)
      .build());

  private byte[] value;
  private java.util.Set<Session> listeners = Sets.newHashSet();

  @Override
  protected void configure(ServiceExecutor executor) {
    executor.register(SET, SERIALIZER::decode, this::set);
    executor.register(GET, this::get, SERIALIZER::encode);
    executor.register(COMPARE_AND_SET, SERIALIZER::decode, this::compareAndSet, SERIALIZER::encode);
    executor.register(GET_AND_SET, SERIALIZER::decode, this::getAndSet, SERIALIZER::encode);
    executor.register(ADD_LISTENER, (Commit<Void> c) -> listeners.add(c.session()));
    executor.register(REMOVE_LISTENER, (Commit<Void> c) -> listeners.remove(c.session()));
  }

  @Override
  public void backup(BufferOutput<?> writer) {
    writer.writeInt(value.length).writeBytes(value);
    java.util.Set<Long> sessionIds = new HashSet<>();
    for (Session session : listeners) {
      sessionIds.add(session.sessionId().id());
    }
    writer.writeObject(sessionIds, SERIALIZER::encode);
  }

  @Override
  public void restore(BufferInput<?> reader) {
    value = reader.readBytes(reader.readInt());
    listeners = new HashSet<>();
    for (Long sessionId : reader.<java.util.Set<Long>>readObject(SERIALIZER::decode)) {
      listeners.add(getSessions().getSession(sessionId));
    }
  }

  private byte[] updateAndNotify(byte[] value) {
    byte[] oldValue = this.value;
    this.value = value;
    AtomicValueEvent<byte[]> event = new AtomicValueEvent<>(oldValue, value);
    listeners.forEach(s -> s.publish(CHANGE, SERIALIZER::encode, event));
    return oldValue;
  }

  /**
   * Handles a set commit.
   *
   * @param commit the commit to handle
   */
  protected void set(Commit<Set> commit) {
    if (!Arrays.equals(this.value, commit.value().value())) {
      updateAndNotify(commit.value().value());
    }
  }

  /**
   * Handles a get commit.
   *
   * @param commit the commit to handle
   * @return value
   */
  protected byte[] get(Commit<Void> commit) {
    return value;
  }

  /**
   * Handles a compare and set commit.
   *
   * @param commit the commit to handle
   * @return indicates whether the value was updated
   */
  protected boolean compareAndSet(Commit<CompareAndSet> commit) {
    if (Arrays.equals(value, commit.value().expect())) {
      updateAndNotify(commit.value().update());
      return true;
    }
    return false;
  }

  /**
   * Handles a get and set commit.
   *
   * @param commit the commit to handle
   * @return value
   */
  protected byte[] getAndSet(Commit<GetAndSet> commit) {
    return updateAndNotify(commit.value().value());
  }
}