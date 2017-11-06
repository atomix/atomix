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
package io.atomix.primitives.value.impl;

import com.google.common.collect.Sets;
import io.atomix.primitives.value.AtomicValueEvent;
import io.atomix.primitives.value.impl.RaftValueOperations.CompareAndSet;
import io.atomix.primitives.value.impl.RaftValueOperations.GetAndSet;
import io.atomix.primitives.value.impl.RaftValueOperations.Set;
import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.util.Arrays;
import java.util.HashSet;

import static io.atomix.primitives.value.impl.RaftValueEvents.CHANGE;
import static io.atomix.primitives.value.impl.RaftValueOperations.ADD_LISTENER;
import static io.atomix.primitives.value.impl.RaftValueOperations.COMPARE_AND_SET;
import static io.atomix.primitives.value.impl.RaftValueOperations.GET;
import static io.atomix.primitives.value.impl.RaftValueOperations.GET_AND_SET;
import static io.atomix.primitives.value.impl.RaftValueOperations.REMOVE_LISTENER;
import static io.atomix.primitives.value.impl.RaftValueOperations.SET;

/**
 * Raft atomic value service.
 */
public class RaftValueService extends AbstractRaftService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftValueOperations.NAMESPACE)
      .register(RaftValueEvents.NAMESPACE)
      .build());

  private byte[] value = new byte[0];
  private java.util.Set<RaftSession> listeners = Sets.newHashSet();

  @Override
  protected void configure(RaftServiceExecutor executor) {
    executor.register(SET, SERIALIZER::decode, this::set);
    executor.register(GET, this::get, SERIALIZER::encode);
    executor.register(COMPARE_AND_SET, SERIALIZER::decode, this::compareAndSet, SERIALIZER::encode);
    executor.register(GET_AND_SET, SERIALIZER::decode, this::getAndSet, SERIALIZER::encode);
    executor.register(ADD_LISTENER, (Commit<Void> c) -> listeners.add(c.session()));
    executor.register(REMOVE_LISTENER, (Commit<Void> c) -> listeners.remove(c.session()));
  }

  @Override
  public void snapshot(SnapshotWriter writer) {
    writer.writeInt(value.length).writeBytes(value);
    java.util.Set<Long> sessionIds = new HashSet<>();
    for (RaftSession session : listeners) {
      sessionIds.add(session.sessionId().id());
    }
    writer.writeObject(sessionIds, SERIALIZER::encode);
  }

  @Override
  public void install(SnapshotReader reader) {
    value = reader.readBytes(reader.readInt());
    listeners = new HashSet<>();
    for (Long sessionId : reader.<java.util.Set<Long>>readObject(SERIALIZER::decode)) {
      listeners.add(sessions().getSession(sessionId));
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