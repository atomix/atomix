// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import java.util.Map;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;

/**
 * Test primary-backup protocol factory.
 */
public class TestLogProtocolFactory {
  private final Map<MemberId, TestLogServerProtocol> servers = Maps.newConcurrentMap();
  private final Map<MemberId, TestLogClientProtocol> clients = Maps.newConcurrentMap();

  /**
   * Returns a new test client protocol.
   *
   * @param memberId the client identifier
   * @return a new test client protocol
   */
  public LogClientProtocol newClientProtocol(MemberId memberId) {
    return new TestLogClientProtocol(memberId, servers, clients);
  }

  /**
   * Returns a new test server protocol.
   *
   * @param memberId the server identifier
   * @return a new test server protocol
   */
  public LogServerProtocol newServerProtocol(MemberId memberId) {
    return new TestLogServerProtocol(memberId, servers, clients);
  }
}
