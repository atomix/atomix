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
package net.kuujo.mimeo.cluster;

import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * A cluster controller.
 * 
 * @author Jordan Halterman
 */
public interface ClusterController {

  /**
   * Cluster membership type.
   * 
   * @author Jordan Halterman
   */
  public static enum MembershipType {

    /**
     * Static cluster membership.
     */
    STATIC,

    /**
     * Dynamic cluster membership.
     */
    DYNAMIC;

  }

  /**
   * Sets the local address.
   * 
   * @param address The address of the local node.
   * @return The cluster controller.
   */
  ClusterController setLocalAddress(String address);

  /**
   * Returns the local address.
   * 
   * @return The address of the local node.
   */
  String getLocalAddress();

  /**
   * Sets the cluster address.
   * 
   * @param address The address of the cluster.
   * @return The cluster controller.
   */
  ClusterController setClusterAddress(String address);

  /**
   * Returns the cluster address.
   * 
   * @return The event bus address of the cluster.
   */
  String getClusterAddress();

  /**
   * Sets the cluster membership type.
   * 
   * @param type The cluster membership type.
   * @return The cluster controller.
   */
  ClusterController setMembershipType(MembershipType type);

  /**
   * Returns the cluster membership type.
   * 
   * @return The cluster membership type.
   */
  MembershipType getMembershipType();

  /**
   * Enables static membership for the cluster.
   *
   * @return
   *   The cluster controller.
   */
  ClusterController enableStaticMembership();

  /**
   * Enables dynamic membership for the cluster.
   *
   * @return
   *   The cluster controller.
   */
  ClusterController enableDynamicMembership();

  /**
   * Adds a member to the cluster.
   * 
   * @param address The cluster member address.
   * @return The cluster controller.
   */
  ClusterController addMember(String address);

  /**
   * Removes a member from the cluster.
   * 
   * @param address The cluster member address.
   * @return The cluster controller.
   */
  ClusterController removeMember(String address);

  /**
   * Sets cluster members.
   * 
   * @param members A list of cluster members.
   * @return The cluster controller.
   */
  ClusterController setMembers(String... members);

  /**
   * Sets cluster members.
   * 
   * @param members A set of cluster members.
   * @return The cluster controller.
   */
  ClusterController setMembers(Set<String> members);

  /**
   * Starts the cluster controller.
   * 
   * @param doneHandler An asynchronous handler to be called once the cluster
   *          has been started.
   * @return The cluster controller.
   */
  ClusterController start(Handler<AsyncResult<ClusterConfig>> doneHandler);

  /**
   * Registers a cluster command message handler.
   * 
   * @param handler A handler to be called when a command message is received by
   *          the cluster.
   * @return The cluster controller.
   */
  ClusterController messageHandler(Handler<Message<JsonObject>> handler);

  /**
   * Stops the cluster controller.
   * 
   * @param doneHandler An asynchronous handler to be called once the cluster
   *          has been stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
