/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.protocol;

import io.atomix.messaging.ClusterCommunicationService;
import io.atomix.serializer.Serializer;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class RaftServerCommunicator implements RaftServerProtocol {
  private final RaftServerProtocolListener listener;
  private final RaftServerProtocolDispatcher dispatcher;

  public RaftServerCommunicator(Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    this(null, serializer, clusterCommunicator);
  }

  public RaftServerCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
    RaftMessageContext context = new RaftMessageContext(prefix);
    this.listener = new RaftServerMessageListener(context, serializer, clusterCommunicator);
    this.dispatcher = new RaftServerMessageDispatcher(context, serializer, clusterCommunicator);
  }

  @Override
  public RaftServerProtocolListener listener() {
    return listener;
  }

  @Override
  public RaftServerProtocolDispatcher dispatcher() {
    return dispatcher;
  }

}
