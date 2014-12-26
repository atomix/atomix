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

import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSet;
import net.kuujo.copycat.collections.internal.collection.DefaultSetState;
import net.kuujo.copycat.collections.internal.collection.SetState;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogConfig;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public interface AsyncSet<T> extends AsyncCollection<T>, AsyncSetProxy<T> {

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new LogConfig(), Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-set-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param config The set configuration.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, LogConfig config) {
    return create(name, uri, cluster, config, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-set-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param executor The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, Executor executor) {
    return create(name, uri, cluster, new LogConfig(), executor);
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param config The set configuration.
   * @param executor The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, LogConfig config, Executor executor) {
    return new DefaultAsyncSet(StateMachine.create(name, uri, SetState.class, new DefaultSetState<>(), cluster, config, executor));
  }

}
