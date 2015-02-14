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
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.concurrent.Executors;

/**
 * Asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The list data type.
 */
public interface AsyncList<T> extends AsyncCollection<AsyncList<T>, T>, AsyncListProxy<T> {

  /**
   * Creates a new asynchronous list with the default cluster configuration.<p>
   *
   * The list will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the list will be constructed with an list configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code list}, {@code list-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the list resource. If the resource is namespaced - e.g. `lists.my-list.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `lists.conf`.
   *
   * @param name The asynchronous list name.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name) {
    return create(name, new ClusterConfig(String.format("%s-cluster", name)), new AsyncListConfig(name));
  }

  /**
   * Creates a new asynchronous list with the default list configuration.<p>
   *
   * The list will be constructed with an list configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code list}, {@code list-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the list resource. If the resource is namespaced - e.g. `lists.my-list.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `lists.conf`.
   *
   * @param name The asynchronous list name.
   * @param cluster The cluster configuration.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, ClusterConfig cluster) {
    return create(name, cluster, new AsyncListConfig(name));
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param cluster The cluster configuration.
   * @param config The list configuration.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T> AsyncList<T> create(String name, ClusterConfig cluster, AsyncListConfig config) {
    return new DefaultAsyncList<>(new ResourceContext(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"))));
  }

}
