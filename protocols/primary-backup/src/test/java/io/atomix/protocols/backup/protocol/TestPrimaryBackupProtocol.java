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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.NodeId;

import java.util.Collection;
import java.util.Map;

/**
 * Base class for Raft protocol.
 */
public abstract class TestPrimaryBackupProtocol {
  private final Map<NodeId, TestPrimaryBackupServerProtocol> servers;
  private final Map<NodeId, TestPrimaryBackupClientProtocol> clients;

  public TestPrimaryBackupProtocol(Map<NodeId, TestPrimaryBackupServerProtocol> servers, Map<NodeId, TestPrimaryBackupClientProtocol> clients) {
    this.servers = servers;
    this.clients = clients;
  }

  TestPrimaryBackupServerProtocol server(NodeId memberId) {
    return servers.get(memberId);
  }

  Collection<TestPrimaryBackupServerProtocol> servers() {
    return servers.values();
  }

  TestPrimaryBackupClientProtocol client(NodeId memberId) {
    return clients.get(memberId);
  }
}
