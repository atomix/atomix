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

import com.google.common.collect.Maps;
import io.atomix.cluster.NodeId;

import java.util.Map;

/**
 * Test primary-backup protocol factory.
 */
public class TestPrimaryBackupProtocolFactory {
  private final Map<NodeId, TestPrimaryBackupServerProtocol> servers = Maps.newConcurrentMap();
  private final Map<NodeId, TestPrimaryBackupClientProtocol> clients = Maps.newConcurrentMap();

  /**
   * Returns a new test client protocol.
   *
   * @param nodeId the client identifier
   * @return a new test client protocol
   */
  public PrimaryBackupClientProtocol newClientProtocol(NodeId nodeId) {
    return new TestPrimaryBackupClientProtocol(nodeId, servers, clients);
  }

  /**
   * Returns a new test server protocol.
   *
   * @param nodeId the server identifier
   * @return a new test server protocol
   */
  public PrimaryBackupServerProtocol newServerProtocol(NodeId nodeId) {
    return new TestPrimaryBackupServerProtocol(nodeId, servers, clients);
  }
}
