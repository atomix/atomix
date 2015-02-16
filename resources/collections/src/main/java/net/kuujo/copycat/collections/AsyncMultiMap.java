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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMap;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.Executor;

/**
 * Asynchronous multi-map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The multimap key type.
 * @param <V> The multimap entry type.
 */
public interface AsyncMultiMap<K, V> extends AsyncMultiMapProxy<K, V>, Resource<AsyncMultiMap<K, V>> {

  /**
   * Creates a new asynchronous multimap, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create() {
    return create(new AsyncMultiMapConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous multimap, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(Executor executor) {
    return create(new AsyncMultiMapConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous multimap, loading the log configuration from the classpath.
   *
   * @param name The asynchronous multimap resource name to be used to load the asynchronous multimap configuration from the classpath.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name) {
    return create(new AsyncMultiMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous multimap, loading the log configuration from the classpath.
   *
   * @param name The asynchronous multimap resource name to be used to load the asynchronous multimap configuration from the classpath.
   * @param executor An executor on which to execute asynchronous multimap callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, Executor executor) {
    return create(new AsyncMultiMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param name The asynchronous multimap resource name to be used to load the asynchronous multimap configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster) {
    return create(new AsyncMultiMapConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param name The asynchronous multimap resource name to be used to load the asynchronous multimap configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous multimap callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncMultiMapConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param config The asynchronous multimap configuration.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(AsyncMultiMapConfig config, ClusterConfig cluster) {
    return new DefaultAsyncMultiMap<>(config, cluster);
  }

  /**
   * Creates a new asynchronous multimap with the given cluster and asynchronous multimap configurations.
   *
   * @param config The asynchronous multimap configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous multimap callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous multimap instance.
   */
  static <K, V> AsyncMultiMap<K, V> create(AsyncMultiMapConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultAsyncMultiMap<>(config, cluster, executor);
  }

}
