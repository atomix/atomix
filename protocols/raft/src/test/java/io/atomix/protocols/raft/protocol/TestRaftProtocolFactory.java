// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.Map;

/**
 * Test Raft protocol factory.
 */
public class TestRaftProtocolFactory {
  private final Map<MemberId, TestRaftServerProtocol> servers = Maps.newConcurrentMap();
  private final Map<MemberId, TestRaftClientProtocol> clients = Maps.newConcurrentMap();
  private final ThreadContext context;

  public TestRaftProtocolFactory(ThreadContext context) {
    this.context = context;
  }

  /**
   * Returns a new test client protocol.
   *
   * @param memberId the client member identifier
   * @return a new test client protocol
   */
  public RaftClientProtocol newClientProtocol(MemberId memberId) {
    return new TestRaftClientProtocol(memberId, servers, clients, context);
  }

  /**
   * Returns a new test server protocol.
   *
   * @param memberId the server member identifier
   * @return a new test server protocol
   */
  public RaftServerProtocol newServerProtocol(MemberId memberId) {
    return new TestRaftServerProtocol(memberId, servers, clients, context);
  }
}
