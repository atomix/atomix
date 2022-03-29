// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Map;

/**
 * Base class for Raft protocol.
 */
public abstract class TestPrimaryBackupProtocol {
  private final Map<MemberId, TestPrimaryBackupServerProtocol> servers;
  private final Map<MemberId, TestPrimaryBackupClientProtocol> clients;

  public TestPrimaryBackupProtocol(Map<MemberId, TestPrimaryBackupServerProtocol> servers, Map<MemberId, TestPrimaryBackupClientProtocol> clients) {
    this.servers = servers;
    this.clients = clients;
  }

  TestPrimaryBackupServerProtocol server(MemberId memberId) {
    return servers.get(memberId);
  }

  Collection<TestPrimaryBackupServerProtocol> servers() {
    return servers.values();
  }

  TestPrimaryBackupClientProtocol client(MemberId memberId) {
    return clients.get(memberId);
  }
}
