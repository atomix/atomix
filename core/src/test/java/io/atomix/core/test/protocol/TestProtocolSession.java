// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
