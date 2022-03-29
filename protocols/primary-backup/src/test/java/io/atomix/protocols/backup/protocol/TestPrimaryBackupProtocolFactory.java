// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
