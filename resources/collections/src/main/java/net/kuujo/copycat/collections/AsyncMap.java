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
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMap;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.Executor;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public interface AsyncMap<K, V> extends AsyncMapProxy<K, V>, Resource<AsyncMap<K, V>> {

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create() {
    return create(new AsyncMapConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(Executor executor) {
    return create(new AsyncMapConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(String name) {
    return create(new AsyncMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous map, loading the log configuration from the classpath.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(String name, Executor executor) {
    return create(new AsyncMapConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(String name, ClusterConfig cluster) {
    return create(new AsyncMapConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param name The asynchronous map resource name to be used to load the asynchronous map configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncMapConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param config The asynchronous map configuration.
   * @param cluster The cluster configuration.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(AsyncMapConfig config, ClusterConfig cluster) {
    return new DefaultAsyncMap<>(config, cluster);
  }

  /**
   * Creates a new asynchronous map with the given cluster and asynchronous map configurations.
   *
   * @param config The asynchronous map configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous map callbacks.
   * @param <K> the map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map instance.
   */
  static <K, V> AsyncMap<K, V> create(AsyncMapConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultAsyncMap<>(config, cluster, executor);
  }

}
