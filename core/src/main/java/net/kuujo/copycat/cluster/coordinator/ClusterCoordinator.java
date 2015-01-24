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
package net.kuujo.copycat.cluster.coordinator;

import net.kuujo.copycat.Managed;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.Cluster;

import java.util.Collection;

/**
 * Cluster coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterCoordinator extends Managed<ClusterCoordinator> {

  /**
   * Returns the coordinator configuration.
   *
   * @return The coordinator configuration.
   */
  CoordinatorConfig config();

  /**
   * Returns the global coordinator cluster.
   *
   * @return The global coordinator cluster.
   */
  Cluster cluster();

  /**
   * Returns the local member coordinator.
   *
   * @return The local member coordinator.
   */
  LocalMemberCoordinator member();

  /**
   * Returns a member coordinator by URI.
   *
   * @param uri The member URI.
   * @return The member coordinator.
   */
  MemberCoordinator member(String uri);

  /**
   * Returns an immutable set of member coordinators.
   *
   * @return An immutable set of member coordinators.
   */
  Collection<MemberCoordinator> members();

  /**
   * Gets a cluster resource.
   *
   * @param name The resource name.
   * @param <T> The resource type.
   * @return The resource instance.
   * @throws net.kuujo.copycat.ConfigurationException If the resource does not exist
   */
  <T extends Resource<T>> T getResource(String name);

  /**
   * Gets a cluster resource.
   *
   * @param name The resource name.
   * @param config The resource configuration.
   * @param <T> The resource type.
   * @return The resource instance.
   * @throws net.kuujo.copycat.ConfigurationException If the resource does not exist
   */
  <T extends Resource<T>> T getResource(String name, CoordinatedResourceConfig config);

}
