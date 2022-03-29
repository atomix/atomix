// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.test.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;

/**
 * Test Raft protocol factory.
 */
public class LocalRaftProtocolFactory {
  private final Serializer serializer;
  private final Map<MemberId, LocalRaftServerProtocol> servers = Maps.newConcurrentMap();
  private final Map<MemberId, LocalRaftClientProtocol> clients = Maps.newConcurrentMap();

  public LocalRaftProtocolFactory(Serializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns a new test client protocol.
   *
   * @param memberId the client member identifier
   * @return a new test client protocol
   */
  public RaftClientProtocol newClientProtocol(MemberId memberId) {
    return new LocalRaftClientProtocol(memberId, serializer, servers, clients);
  }

  /**
   * Returns a new test server protocol.
   *
   * @param memberId the server member identifier
   * @return a new test server protocol
   */
  public RaftServerProtocol newServerProtocol(MemberId memberId) {
    return new LocalRaftServerProtocol(memberId, serializer, servers, clients);
  }
}
