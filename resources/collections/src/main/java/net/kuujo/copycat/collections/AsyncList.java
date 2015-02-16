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
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncList;

import java.util.concurrent.Executor;

/**
 * Asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The list data type.
 */
public interface AsyncList<T> extends AsyncCollection<AsyncList<T>, T>, AsyncListProxy<T> {

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create() {
    return create(new AsyncListConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(Executor executor) {
    return create(new AsyncListConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(String name) {
    return create(new AsyncListConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(String name, Executor executor) {
    return create(new AsyncListConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(String name, ClusterConfig cluster) {
    return create(new AsyncListConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncListConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param config The asynchronous list configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(AsyncListConfig config, ClusterConfig cluster) {
    return new DefaultAsyncList<>(config, cluster);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param config The asynchronous list configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @return A new asynchronous list instance.
   */
  static <T> AsyncList<T> create(AsyncListConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultAsyncList<>(config, cluster, executor);
  }

}
