// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import java.util.Collection;
import java.util.Map;

import io.atomix.cluster.MemberId;

/**
 * Base class for Raft protocol.
 */
public abstract class TestLogProtocol {
  private final Map<MemberId, TestLogServerProtocol> servers;
  private final Map<MemberId, TestLogClientProtocol> clients;

  public TestLogProtocol(Map<MemberId, TestLogServerProtocol> servers, Map<MemberId, TestLogClientProtocol> clients) {
    this.servers = servers;
    this.clients = clients;
  }

  TestLogServerProtocol server(MemberId memberId) {
    return servers.get(memberId);
  }

  Collection<TestLogServerProtocol> servers() {
    return servers.values();
  }

  TestLogClientProtocol client(MemberId memberId) {
    return clients.get(memberId);
  }
}
