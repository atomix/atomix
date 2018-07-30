/*
 * Copyright 2016 Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.atomix.core.multimap;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * A builder class for {@code AsyncConsistentMultimap}.
 */
public abstract class AtomicMultimapBuilder<K, V>
    extends MultimapBuilder<AtomicMultimapBuilder<K, V>, AtomicMultimapConfig, AtomicMultimap<K, V>, K, V>
    implements ProxyCompatibleBuilder<AtomicMultimapBuilder<K, V>> {

  protected AtomicMultimapBuilder(String name, AtomicMultimapConfig config, PrimitiveManagementService managementService) {
    super(AtomicMultimapType.instance(), name, config, managementService);
  }

  @Override
  public AtomicMultimapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
