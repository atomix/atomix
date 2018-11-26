/*
 * Copyright 2018-present Open Networking Foundation
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
 * limitations under the License.
 */
package io.atomix.primitive.session.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Serializer;

import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract session.
 */
public abstract class AbstractSession<C> implements Session<C> {
  private final SessionId sessionId;
  private final String primitiveName;
  private final PrimitiveType primitiveType;
  private final MemberId memberId;
  private final Serializer serializer;

  @SuppressWarnings("unchecked")
  protected AbstractSession(
      SessionId sessionId,
      String primitiveName,
      PrimitiveType primitiveType,
      MemberId memberId,
      Serializer serializer) {
    this.sessionId = checkNotNull(sessionId);
    this.primitiveName = checkNotNull(primitiveName);
    this.primitiveType = checkNotNull(primitiveType);
    this.memberId = memberId;
    this.serializer = checkNotNull(serializer);
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String primitiveName() {
    return primitiveName;
  }

  @Override
  public PrimitiveType primitiveType() {
    return primitiveType;
  }

  @Override
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Encodes the given object using the configured {@link #serializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object) {
    return object != null ? serializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  protected <T> T decode(byte[] bytes) {
    return bytes != null ? serializer.decode(bytes) : null;
  }

  @Override
  public abstract void publish(PrimitiveEvent event);

  @Override
  public void publish(EventType eventType, Object event) {
    publish(PrimitiveEvent.event(eventType, encode(event)));
  }

  @Override
  public void accept(Consumer event) {
    throw new UnsupportedOperationException();
  }
}
