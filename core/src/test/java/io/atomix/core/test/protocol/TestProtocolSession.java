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
package io.atomix.core.test.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.impl.AbstractSession;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.serializer.Serializer;

/**
 * Test protocol session.
 */
public class TestProtocolSession<C> extends AbstractSession<C> {
  private final TestSessionClient client;
  private final ThreadContext context;
  private volatile State state = State.CLOSED;

  public TestProtocolSession(
      SessionId sessionId,
      String primitiveName,
      PrimitiveType primitiveType,
      MemberId memberId,
      Serializer serializer,
      TestSessionClient client,
      ThreadContext context) {
    super(sessionId, primitiveName, primitiveType, memberId, serializer);
    this.client = client;
    this.context = context;
  }

  void setState(State state) {
    this.state = state;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void publish(PrimitiveEvent event) {
    context.execute(() -> client.accept(event));
  }
}
