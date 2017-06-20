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
import io.atomix.utils.Identifier;

/**
 * Gossip protocol dispatcher.
 */
public interface GossipProtocolDispatcher<T extends Identifier> {

  /**
   * Sends a gossip message to the given node.
   *
   * @param identifier the location to which to send the gossip message
   * @param message the gossip message to send
   */
  void gossip(T identifier, GossipMessage message);

}
