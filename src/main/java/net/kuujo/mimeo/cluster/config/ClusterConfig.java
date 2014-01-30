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
package net.kuujo.mimeo.cluster.config;

import java.util.Observable;
import java.util.Set;

/**
 * A cluster configuration.
 *
 * @author Jordan Halterman
 */
public abstract class ClusterConfig extends Observable {

  /**
   * Sets the cluster members.
   *
   * @param members
   *   A list of cluster members.
   * @return
   *   The cluster configuration.
   */
  public abstract ClusterConfig setMembers(String... members);

  /**
   * Sets the cluster members.
   *
   * @param members
   *   A set of cluster members.
   * @return
   *   The cluster configuration.
   */
  public abstract ClusterConfig setMembers(Set<String> members);

  /**
   * Adds a member to the cluster.
   *
   * @param address
   *   The cluster member address.
   * @return
   *   The cluster configuration.
   */
  public abstract ClusterConfig addMember(String address);

  /**
   * Returns a boolean indicating whether the cluster has a member.
   *
   * @param address
   *   The cluster member address.
   * @return
   *   Indicates whether the cluster has a member.
   */
  public abstract boolean hasMember(String address);

  /**
   * Removes a member from the cluster.
   *
   * @param address
   *   The address of the member to remove.
   * @return
   *   The cluster configuration.
   */
  public abstract ClusterConfig removeMember(String address);

  /**
   * Returns a set of cluster members.
   *
   * @return
   *   A set of cluster member addresses.
   */
  public abstract Set<String> getMembers();

  /**
   * Locks the configuration once the cluster has been started.
   */
  public abstract void lock();

}
