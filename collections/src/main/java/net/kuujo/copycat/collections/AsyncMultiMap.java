/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.map.MultiMapState;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMap;
import net.kuujo.copycat.collections.internal.map.DefaultMultiMapState;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogConfig;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Asynchronous multi-map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public interface AsyncMultiMap<K, V> extends AsyncMultiMapProxy<K, V>, CopycatResource {

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param uri The asynchronous multimap member URI.
   * @param cluster The cluster configuration.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new LogConfig(), Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-multimap-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param uri The asynchronous multimap member URI.
   * @param cluster The cluster configuration.
   * @param config The multimap configuration.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, String uri, ClusterConfig cluster, LogConfig config) {
    return create(name, uri, cluster, config, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-multimap-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param uri The asynchronous multimap member URI.
   * @param cluster The cluster configuration.
   * @param executor The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, String uri, ClusterConfig cluster, Executor executor) {
    return create(name, uri, cluster, new LogConfig(), executor);
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param uri The asynchronous multimap member URI.
   * @param cluster The cluster configuration.
   * @param config The multimap configuration.
   * @param executor The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, String uri, ClusterConfig cluster, LogConfig config, Executor executor) {
    return new DefaultAsyncMultiMap(StateMachine.create(name, uri, MultiMapState.class, new DefaultMultiMapState<>(), cluster, config, executor));
  }

}
