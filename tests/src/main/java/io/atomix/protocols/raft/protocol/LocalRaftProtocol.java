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

import io.atomix.protocols.raft.cluster.MemberId;

import java.util.Collection;
import java.util.Map;

/**
 * Base class for Raft protocol.
 */
public abstract class LocalRaftProtocol {
  private final Map<MemberId, LocalRaftServerProtocol> servers;
  private final Map<MemberId, LocalRaftClientProtocol> clients;

  public LocalRaftProtocol(Map<MemberId, LocalRaftServerProtocol> servers, Map<MemberId, LocalRaftClientProtocol> clients) {
    this.servers = servers;
    this.clients = clients;
  }

  LocalRaftServerProtocol server(MemberId memberId) {
    return servers.get(memberId);
  }

  Collection<LocalRaftServerProtocol> servers() {
    return servers.values();
  }

  LocalRaftClientProtocol client(MemberId memberId) {
    return clients.get(memberId);
  }
}
