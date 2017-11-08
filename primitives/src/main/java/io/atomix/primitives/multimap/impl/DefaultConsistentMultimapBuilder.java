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

package io.atomix.primitives.multimap.impl;

import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.multimap.AsyncConsistentMultimap;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;

/**
 * Default {@link AsyncConsistentMultimap} builder.
 */
public class DefaultConsistentMultimapBuilder<K, V> extends ConsistentMultimapBuilder<K, V> {

  private final DistributedPrimitiveCreator primitiveCreator;

  public DefaultConsistentMultimapBuilder(
      DistributedPrimitiveCreator primitiveCreator) {
    this.primitiveCreator = primitiveCreator;
  }

  @Override
  public AsyncConsistentMultimap<K, V> buildAsync() {
    return primitiveCreator.newAsyncConsistentSetMultimap(name(), serializer());
  }
}
