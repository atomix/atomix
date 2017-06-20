/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.gossip.protocol;

import io.atomix.cluster.NodeId;

import java.util.concurrent.CompletableFuture;

/**
 * Anti-entropy protocol dispatcher.
 */
public interface AntiEntropyProtocolDispatcher extends GossipProtocolDispatcher {

  /**
   * Sends an anti-entropy advertisement.
   *
   * @param nodeId the node ID to which to send the advertisement
   * @param advertisement the anti-entropy advertisement to send
   * @return a future to be completed with the advertisement response
   */
  <K> CompletableFuture<AntiEntropyResponse<K>> advertise(NodeId nodeId, AntiEntropyAdvertisement<K> advertisement);

}
