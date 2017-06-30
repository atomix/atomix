/*
 * Copyright 2015-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.proxy;

import io.atomix.protocols.raft.cluster.MemberId;

import java.util.List;

/**
 * Strategy for selecting nodes to which to connect and submit operations.
 * <p>
 * Selection strategies prioritize communication with certain servers over others. When the client
 * loses its connection or cluster membership changes, the client will request a list of servers to
 * which the client can connect. The address list should be prioritized.
 */
public interface CommunicationStrategy {

  /**
   * Returns a prioritized list of servers to which the client can connect and submit operations.
   * <p>
   * The client will iterate the provided {@link MemberId} list in order, attempting to connect to
   * each listed server until all servers have been exhausted. Implementations should provide a
   * complete list of servers with which the client can communicate. Limiting the server list
   * only to a single server such as the {@code leader} may result in the client failing, such as in
   * the event that no leader exists or the client is partitioned from the leader.
   *
   * @param leader  The current cluster leader. The {@code leader} may be {@code null} if no current
   *                leader exists.
   * @param servers The full list of available servers. The provided server list is representative
   *                of the most recent membership update received by the client. The server list
   *                may evolve over time as the structure of the cluster changes.
   * @return A collection of servers to which the client can connect.
   */
  List<MemberId> selectConnections(MemberId leader, List<MemberId> servers);

}
