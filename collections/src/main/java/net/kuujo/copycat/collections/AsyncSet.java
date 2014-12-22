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
import net.kuujo.copycat.collections.internal.collection.AsyncSetState;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSet;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSetState;
import net.kuujo.copycat.spi.ExecutionContext;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public interface AsyncSet<T> extends AsyncCollection<T> {

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncSetConfig(String.format("copycat.set.%s", name)), ExecutionContext.create());
  }

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
    return create(name, uri, cluster, new AsyncSetConfig(String.format("copycat.set.%s", name)), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param config The set configuration.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, AsyncSetConfig config) {
    return create(name, uri, new ClusterConfig(), config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param context The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), new AsyncSetConfig(String.format("copycat.set.%s", name)), context);
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
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, AsyncSetConfig config) {
    return create(name, uri, cluster, config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param context The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, ExecutionContext context) {
    return create(name, uri, cluster, new AsyncSetConfig(String.format("copycat.set.%s", name)), context);
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param config The set configuration.
   * @param context The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, AsyncSetConfig config, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), config, context);
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param config The set configuration.
   * @param context The user execution context.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, AsyncSetConfig config, ExecutionContext context) {
    return new DefaultAsyncSet(StateMachine.create(name, uri, AsyncSetState.class, new DefaultAsyncSetState<>(), cluster, config, context));
  }

}
