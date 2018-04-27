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
import io.atomix.cluster.MemberId;

import java.util.Map;

/**
 * Test primary-backup protocol factory.
 */
public class TestPrimaryBackupProtocolFactory {
  private final Map<MemberId, TestPrimaryBackupServerProtocol> servers = Maps.newConcurrentMap();
  private final Map<MemberId, TestPrimaryBackupClientProtocol> clients = Maps.newConcurrentMap();

  /**
   * Returns a new test client protocol.
   *
   * @param memberId the client identifier
   * @return a new test client protocol
   */
  public PrimaryBackupClientProtocol newClientProtocol(MemberId memberId) {
    return new TestPrimaryBackupClientProtocol(memberId, servers, clients);
  }

  /**
   * Returns a new test server protocol.
   *
   * @param memberId the server identifier
   * @return a new test server protocol
   */
  public PrimaryBackupServerProtocol newServerProtocol(MemberId memberId) {
    return new TestPrimaryBackupServerProtocol(memberId, servers, clients);
  }
}
