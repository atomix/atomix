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
package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;

/**
 * CRDT protocol builder.
 */
@Beta
public class CrdtProtocolBuilder extends PrimitiveProtocolBuilder<CrdtProtocolBuilder, CrdtProtocolConfig, CrdtProtocol> {
  public CrdtProtocolBuilder(CrdtProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the CRDT protocol builder
   */
  public CrdtProtocolBuilder withTimestampProvider(TimestampProvider timestampProvider) {
    config.setTimestampProvider(timestampProvider);
    return this;
  }

  /**
   * Sets the gossip interval.
   *
   * @param gossipInterval the gossip interval
   * @return the CRDT protocol builder
   */
  public CrdtProtocolBuilder withGossipInterval(Duration gossipInterval) {
    config.setGossipInterval(gossipInterval);
    return this;
  }

  @Override
  public CrdtProtocol build() {
    return new CrdtProtocol(config);
  }
}
