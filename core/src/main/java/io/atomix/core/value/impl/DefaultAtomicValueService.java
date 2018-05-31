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
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Raft atomic value service.
 */
public class DefaultAtomicValueService extends AbstractPrimitiveService<AtomicValueClient, ServiceConfig> implements AtomicValueService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicValueOperations.NAMESPACE)
      .register(AtomicValueEvents.NAMESPACE)
      .build());

  private byte[] value;
  private java.util.Set<PrimitiveSession> listeners = Sets.newHashSet();

  public DefaultAtomicValueService(ServiceConfig config) {
    super(AtomicValueClient.class, config);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeInt(value.length).writeBytes(value);
    java.util.Set<Long> sessionIds = new HashSet<>();
    for (PrimitiveSession session : listeners) {
      sessionIds.add(session.sessionId().id());
    }
    writer.writeObject(sessionIds);
  }

  @Override
  public void restore(BackupInput reader) {
    value = reader.readBytes(reader.readInt());
    listeners = new HashSet<>();
    for (Long sessionId : reader.<java.util.Set<Long>>readObject()) {
      listeners.add(getSession(sessionId));
    }
  }

  private byte[] updateAndNotify(byte[] value) {
    byte[] oldValue = this.value;
    this.value = value;
    listeners.forEach(s -> acceptOn(s, client -> client.change(value, oldValue)));
    return oldValue;
  }

  @Override
  public void set(byte[] value) {
    if (!Arrays.equals(this.value, value)) {
      updateAndNotify(value);
    }
  }

  @Override
  public byte[] get() {
    return value;
  }

  @Override
  public boolean compareAndSet(byte[] expect, byte[] update) {
    if (Arrays.equals(value, expect)) {
      updateAndNotify(update);
      return true;
    }
    return false;
  }

  @Override
  public byte[] getAndSet(byte[] value) {
    if (!Arrays.equals(this.value, value)) {
      return updateAndNotify(value);
    }
    return this.value;
  }

  @Override
  public void addListener() {
    listeners.add(getCurrentSession());
  }

  @Override
  public void removeListener() {
    listeners.remove(getCurrentSession());
  }
}