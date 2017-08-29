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
package io.atomix.protocols.gossip.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Anti-entropy advertisement.
 */
public class AntiEntropyAdvertisement<K> {
  private final Map<K, GossipUpdate.Digest> digest;

  /**
   * Creates a new anti entropy advertisement message.
   *
   * @param digest for map entries
   */
  public AntiEntropyAdvertisement(Map<K, GossipUpdate.Digest> digest) {
    this.digest = ImmutableMap.copyOf(checkNotNull(digest));
  }

  /**
   * Returns the digest for map entries.
   *
   * @return mapping from key to associated digest
   */
  public Map<K, GossipUpdate.Digest> digest() {
    return digest;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("totalEntries", digest.size())
        .toString();
  }
}
