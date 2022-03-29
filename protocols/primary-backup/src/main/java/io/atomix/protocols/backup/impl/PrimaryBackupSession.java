// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.impl.AbstractSession;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

/**
 * Primary-backup session.
 */
public class PrimaryBackupSession extends AbstractSession {
  private final Logger log;
  private final PrimaryBackupServiceContext context;
  private State state = State.OPEN;

  public PrimaryBackupSession(SessionId sessionId, MemberId memberId, Serializer serializer, PrimaryBackupServiceContext context) {
    super(sessionId, context.serviceName(), context.serviceType(), memberId, serializer);
    this.context = context;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(context.serverName())
        .add("session", sessionId)
        .build());
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void publish(PrimitiveEvent event) {
    if (context.getRole() == Role.PRIMARY) {
      context.threadContext().execute(() -> {
        log.trace("Sending {} to {}", event, memberId());
        context.protocol().event(memberId(), sessionId(), event);
      });
    }
  }

  public void expire() {
    state = State.EXPIRED;
  }

  public void close() {
    state = State.CLOSED;
  }
}
