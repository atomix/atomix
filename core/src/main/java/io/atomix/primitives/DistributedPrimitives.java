/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitives;

import io.atomix.primitives.map.AsyncAtomicCounterMap;
import io.atomix.primitives.map.AsyncConsistentMap;
import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.impl.CachingAsyncConsistentMap;
import io.atomix.primitives.map.impl.NotNullAsyncConsistentMap;
import io.atomix.primitives.map.impl.TranscodingAsyncAtomicCounterMap;
import io.atomix.primitives.map.impl.TranscodingAsyncConsistentMap;
import io.atomix.primitives.map.impl.TranscodingAsyncConsistentTreeMap;
import io.atomix.primitives.map.impl.UnmodifiableAsyncConsistentMap;
import io.atomix.primitives.multimap.AsyncConsistentMultimap;
import io.atomix.primitives.multimap.impl.TranscodingAsyncConsistentMultimap;
import io.atomix.primitives.set.AsyncDistributedSet;
import io.atomix.primitives.set.impl.DelegatingAsyncDistributedSet;
import io.atomix.primitives.tree.AsyncDocumentTree;
import io.atomix.primitives.tree.impl.CachingAsyncDocumentTree;

import java.util.function.Function;

/**
 * Misc utilities for working with {@code DistributedPrimitive}s.
 */
public final class DistributedPrimitives {

  private DistributedPrimitives() {
  }

  /**
   * Creates an instance of {@code AsyncDistributedSet} that is backed by a {@code AsyncConsistentMap}.
   *
   * @param map backing map
   * @param <E> set element type
   * @return set
   */
  public static <E> AsyncDistributedSet<E> newSetFromMap(AsyncConsistentMap<E, Boolean> map) {
    return new DelegatingAsyncDistributedSet<>(map);
  }

  /**
   * Creates an instance of {@code AsyncConsistentMap} that caches entries on get.
   *
   * @param map backing map
   * @param <K> map key type
   * @param <V> map value type
   * @return caching map
   */
  public static <K, V> AsyncConsistentMap<K, V> newCachingMap(AsyncConsistentMap<K, V> map) {
    return new CachingAsyncConsistentMap<>(map);
  }

  /**
   * Creates an instance of {@code AsyncConsistentMap} that disallows updates.
   *
   * @param map backing map
   * @param <K> map key type
   * @param <V> map value type
   * @return unmodifiable map
   */
  public static <K, V> AsyncConsistentMap<K, V> newUnmodifiableMap(AsyncConsistentMap<K, V> map) {
    return new UnmodifiableAsyncConsistentMap<>(map);
  }

  /**
   * Creates an instance of {@code AsyncConsistentMap} that disallows null values.
   *
   * @param map backing map
   * @param <K> map key type
   * @param <V> map value type
   * @return not null map
   */
  public static <K, V> AsyncConsistentMap<K, V> newNotNullMap(AsyncConsistentMap<K, V> map) {
    return new NotNullAsyncConsistentMap<>(map);
  }

  /**
   * Creates an instance of {@code AsyncAtomicCounterMap} that transforms key types.
   *
   * @param map        backing map
   * @param keyEncoder transformer for key type of returned map to key type of input map
   * @param keyDecoder transformer for key type of input map to key type of returned map
   * @param <K1>       returned map key type
   * @param <K2>       input map key type
   * @return new counter map
   */
  public static <K1, K2> AsyncAtomicCounterMap<K1> newTranscodingAtomicCounterMap(AsyncAtomicCounterMap<K2> map,
                                                                                  Function<K1, K2> keyEncoder,
                                                                                  Function<K2, K1> keyDecoder) {
    return new TranscodingAsyncAtomicCounterMap<>(map, keyEncoder, keyDecoder);
  }

  /**
   * Creates an instance of {@code AsyncConsistentMap} that transforms operations inputs and applies them
   * to corresponding operation in a different typed map and returns the output after reverse transforming it.
   *
   * @param map          backing map
   * @param keyEncoder   transformer for key type of returned map to key type of input map
   * @param keyDecoder   transformer for key type of input map to key type of returned map
   * @param valueEncoder transformer for value type of returned map to value type of input map
   * @param valueDecoder transformer for value type of input map to value type of returned map
   * @param <K1>         returned map key type
   * @param <K2>         input map key type
   * @param <V1>         returned map value type
   * @param <V2>         input map key type
   * @return new map
   */
  public static <K1, V1, K2, V2> AsyncConsistentMap<K1, V1> newTranscodingMap(AsyncConsistentMap<K2, V2> map,
                                                                              Function<K1, K2> keyEncoder,
                                                                              Function<K2, K1> keyDecoder,
                                                                              Function<V1, V2> valueEncoder,
                                                                              Function<V2, V1> valueDecoder) {
    return new TranscodingAsyncConsistentMap<K1, V1, K2, V2>(map,
        keyEncoder,
        keyDecoder,
        valueEncoder,
        valueDecoder);
  }

  /**
   * Creates an instance of {@code DistributedTreeMap} that transforms operations inputs and applies them
   * to corresponding operation in a different typed map and returns the output after reverse transforming it.
   *
   * @param map          backing map
   * @param valueEncoder transformer for value type of returned map to value type of input map
   * @param valueDecoder transformer for value type of input map to value type of returned map
   * @param <V1>         returned map value type
   * @param <V2>         input map key type
   * @return new map
   */
  public static <V1, V2> AsyncConsistentTreeMap<V1> newTranscodingTreeMap(
      AsyncConsistentTreeMap<V2> map,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    return new TranscodingAsyncConsistentTreeMap<>(map,
        valueEncoder,
        valueDecoder);
  }

  /**
   * Creates an instance of {@code AsyncConsistentMultimap} that transforms
   * operations inputs and applies them to corresponding operation in a
   * differently typed map and returns the output after reverse transforming
   * it.
   *
   * @param multimap     backing multimap
   * @param keyEncoder   transformer for key type of returned map to key type
   *                     of input map
   * @param keyDecoder   transformer for key type of input map to key type of
   *                     returned map
   * @param valueEncoder transformer for value type of returned map to value
   *                     type of input map
   * @param valueDecoder transformer for value type of input map to value
   *                     type of returned map
   * @param <K1>         returned map key type
   * @param <K2>         input map key type
   * @param <V1>         returned map value type
   * @param <V2>         input map key type
   * @return new map
   */
  public static <K1, V1, K2, V2> AsyncConsistentMultimap<K1, V1> newTranscodingMultimap(AsyncConsistentMultimap<K2, V2> multimap,
                                                                                        Function<K1, K2> keyEncoder,
                                                                                        Function<K2, K1> keyDecoder,
                                                                                        Function<V1, V2> valueEncoder,
                                                                                        Function<V2, V1> valueDecoder) {
    return new TranscodingAsyncConsistentMultimap<>(multimap,
        keyEncoder,
        keyDecoder,
        valueDecoder,
        valueEncoder);
  }

  /**
   * Creates an instance of {@code AsyncDocumentTree} that caches values on get.
   *
   * @param tree backing tree
   * @param <V>  tree value type
   * @return caching tree
   */
  public static <V> AsyncDocumentTree<V> newCachingDocumentTree(AsyncDocumentTree<V> tree) {
    return new CachingAsyncDocumentTree<V>(tree);
  }

}
