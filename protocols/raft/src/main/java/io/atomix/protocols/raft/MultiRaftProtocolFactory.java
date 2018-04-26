/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolFactory;

/**
 * Raft protocol factory.
 */
public class MultiRaftProtocolFactory implements PrimitiveProtocolFactory<MultiRaftProtocolConfig, MultiRaftProtocol> {
  @Override
  public PrimitiveProtocol.Type type() {
    return MultiRaftProtocol.TYPE;
  }

  @Override
  public MultiRaftProtocol create(MultiRaftProtocolConfig config) {
    return new MultiRaftProtocol(config);
  }
}
