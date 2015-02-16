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
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSet;

import java.util.concurrent.Executor;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public interface AsyncSet<T> extends AsyncCollection<AsyncSet<T>, T>, AsyncSetProxy<T> {

  /**
   * Creates a new asynchronous set, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous set entry type.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create() {
    return create(new AsyncSetConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous set, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous set entry type.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(Executor executor) {
    return create(new AsyncSetConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous set, loading the log configuration from the classpath.
   *
   * @param name The asynchronous set resource name to be used to load the asynchronous set configuration from the classpath.
   * @param <T> The asynchronous set entry type.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(String name) {
    return create(new AsyncSetConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous set, loading the log configuration from the classpath.
   *
   * @param name The asynchronous set resource name to be used to load the asynchronous set configuration from the classpath.
   * @param executor An executor on which to execute asynchronous set callbacks.
   * @param <T> The asynchronous set entry type.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(String name, Executor executor) {
    return create(new AsyncSetConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param name The asynchronous set resource name to be used to load the asynchronous set configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(String name, ClusterConfig cluster) {
    return create(new AsyncSetConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param name The asynchronous set resource name to be used to load the asynchronous set configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous set callbacks.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncSetConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param config The asynchronous set configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(AsyncSetConfig config, ClusterConfig cluster) {
    return new DefaultAsyncSet<>(config, cluster);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param config The asynchronous set configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous set callbacks.
   * @return A new asynchronous set instance.
   */
  static <T> AsyncSet<T> create(AsyncSetConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultAsyncSet<>(config, cluster, executor);
  }

}
