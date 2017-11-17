/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.impl;

import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionEvent;
import io.atomix.primitive.session.SessionEvent.Type;
import io.atomix.primitive.session.SessionEventListener;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.KryoNamespaces;

import java.util.Set;

/**
 * Primary-backup session.
 */
public class PrimaryBackupSession implements Session {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC); // TODO
  private final String nodeName;
  private final SessionId sessionId;
  private final String serviceName;
  private final PrimitiveType primitiveType;
  private final NodeId nodeId;
  private final ClusterService clusterService;
  private final ClusterCommunicationService clusterCommunicator;
  private final MessageSubject eventSubject;
  private final Set<SessionEventListener> eventListeners = Sets.newIdentityHashSet();
  private State state = State.OPEN;

  public PrimaryBackupSession(
      String serverName,
      SessionId sessionId,
      String serviceName,
      PrimitiveType primitiveType,
      NodeId nodeId,
      ClusterService clusterService,
      ClusterCommunicationService clusterCommunicator) {
    this.nodeName = serverName;
    this.sessionId = sessionId;
    this.serviceName = serviceName;
    this.primitiveType = primitiveType;
    this.nodeId = nodeId;
    this.clusterService = clusterService;
    this.clusterCommunicator = clusterCommunicator;
    this.eventSubject = new MessageSubject(String.format("%s-%s-%s", serverName, serviceName, sessionId));
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public NodeId nodeId() {
    return nodeId;
  }

  @Override
  public long minTimeout() {
    return 0; // TODO
  }

  @Override
  public long maxTimeout() {
    return 0; // TODO
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addListener(SessionEventListener listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(SessionEventListener listener) {
    eventListeners.remove(listener);
  }

  @Override
  public void publish(PrimitiveEvent event) {
    clusterCommunicator.unicast(eventSubject, event, SERIALIZER::encode, nodeId);
  }

  void expire() {
    state = State.EXPIRED;
    eventListeners.forEach(l -> l.onEvent(new SessionEvent(Type.EXPIRE, this)));
  }

  void close() {
    state = State.CLOSED;
    eventListeners.forEach(l -> l.onEvent(new SessionEvent(Type.CLOSE, this)));
  }
}
