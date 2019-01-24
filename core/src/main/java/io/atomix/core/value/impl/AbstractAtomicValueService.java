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
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Arrays;
import java.util.Set;

/**
 * Abstract atomic value service.
 */
public abstract class AbstractAtomicValueService extends AbstractPrimitiveService<AtomicValueClient> implements AtomicValueService {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(AtomicValueType.instance().namespace())
      .register(SessionId.class)
      .build());

  private byte[] value;
  private Set<SessionId> listeners = Sets.newHashSet();

  protected AbstractAtomicValueService(PrimitiveType primitiveType) {
    super(primitiveType, AtomicValueClient.class);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    byte[] value = this.value;
    if (value == null) {
      value = new byte[0];
    }
    writer.writeInt(value.length).writeBytes(value);
    writer.writeObject(listeners);
  }

  @Override
  public void restore(BackupInput reader) {
    value = reader.readBytes(reader.readInt());
    if (value.length == 0) {
      value = null;
    }
    listeners = reader.readObject();
  }

  private byte[] updateAndNotify(byte[] value) {
    byte[] oldValue = this.value;
    this.value = value;
    listeners.forEach(session -> getSession(session).accept(client -> client.change(value, oldValue)));
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
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void removeListener() {
    listeners.remove(getCurrentSession().sessionId());
  }

  @Override
  protected void onExpire(Session session) {
    listeners.remove(session.sessionId());
  }

  @Override
  protected void onClose(Session session) {
    listeners.remove(session.sessionId());
  }
}
