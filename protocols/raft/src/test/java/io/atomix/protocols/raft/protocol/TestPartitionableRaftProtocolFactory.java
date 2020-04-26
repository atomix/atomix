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
package io.atomix.protocols.raft.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.ThreadContext;
import java.util.Map;

public class TestPartitionableRaftProtocolFactory {

  private final Map<MemberId, Map<MemberId, TestRaftServerProtocol>> serverConnections =
      Maps.newConcurrentMap();
  private final Map<MemberId, TestRaftClientProtocol> clients;
  private final Map<MemberId, TestRaftServerProtocol> servers;
  private final ThreadContext context;

  public TestPartitionableRaftProtocolFactory(TestRaftProtocolFactory protocolFactory) {
    this.context = protocolFactory.context;
    this.servers = protocolFactory.servers;
    this.clients = protocolFactory.clients;
  }

  public TestRaftServerProtocol newServerProtocol(MemberId memberId) {
    final TestRaftServerProtocol protocol =
        new TestRaftServerProtocol(memberId, getConnections(memberId), clients, context);
    servers.put(memberId, protocol);
    return protocol;
  }

  public void connectAll() {
    servers.forEach((member, protocol) -> connectAll(member));
  }

  private void connectAll(MemberId server) {
    servers.forEach((member, protocol) -> connect(server, member));
  }

  private Map<MemberId, TestRaftServerProtocol> getConnections(MemberId memberId) {
    return serverConnections.computeIfAbsent(memberId, m -> Maps.newConcurrentMap());
  }

  public void partition(MemberId server1, MemberId server2) {
    disconnect(server1, server2);
    disconnect(server2, server1);
  }

  public void heal(MemberId server1, MemberId server2) {
    connect(server1, server2);
    connect(server2, server1);
  }

  private void disconnect(MemberId source, MemberId dest) {
    getConnections(source).remove(dest);
  }

  private void connect(MemberId source, MemberId dest) {
    getConnections(source).put(dest, servers.get(dest));
  }

  public TestRaftServerProtocol getServer(MemberId memberId) {
    return servers.get(memberId);
  }
}
