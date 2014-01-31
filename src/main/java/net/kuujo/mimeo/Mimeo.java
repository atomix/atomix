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
package net.kuujo.mimeo;

import net.kuujo.mimeo.cluster.ClusterController;
import net.kuujo.mimeo.cluster.impl.DefaultClusterController;
import net.kuujo.mimeo.impl.DefaultNode;
import net.kuujo.mimeo.impl.DefaultService;
import net.kuujo.mimeo.impl.DefaultServiceEndpoint;
import net.kuujo.mimeo.log.Log;

import org.vertx.java.core.Vertx;
import org.vertx.java.platform.Verticle;

/**
 * Core Mimeo API.
 * 
 * @author Jordan Halterman
 */
public class Mimeo {
  private final Vertx vertx;

  public Mimeo(Verticle verticle) {
    this(verticle.getVertx());
  }

  public Mimeo(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates a new node.
   *
   * @return
   *   A new node instance.
   */
  public Node createNode() {
    return new DefaultNode(vertx);
  }

  /**
   * Creates a new node.
   *
   * @param address
   *   The node address.
   * @return
   *   A new node instance.
   */
  public Node createNode(String address) {
    return new DefaultNode(address, vertx);
  }

  /**
   * Creates a new cluster controller.
   *
   * @return
   *   A new cluster controller.
   */
  public ClusterController createCluster() {
    return new DefaultClusterController(vertx);
  }

  /**
   * Creates a new cluster controller.
   *
   * @param localAddress
   *   The local address.
   * @param broadcastAddress
   *   The cluster broadcast address.
   * @return
   *   A new cluster controller.
   */
  public ClusterController createCluster(String localAddress, String broadcastAddress) {
    return new DefaultClusterController(localAddress, broadcastAddress, vertx);
  }

  /**
   * Creates a new service instance.
   *
   * @return
   *   A new service instance.
   */
  public Service createService() {
    return new DefaultService(vertx);
  }

  /**
   * Creates a new service instance.
   *
   * @param address
   *   The service address.
   * @return
   *   A new service instance.
   */
  public Service createService(String address) {
    return new DefaultService(address, vertx);
  }

  /**
   * Creates a new service instance.
   *
   * @param address
   *   The service address.
   * @param log
   *   The replicated log.
   * @return
   *   A new service instance.
   */
  public Service createService(String address, Log log) {
    return new DefaultService(address, vertx, log);
  }

  /**
   * Creates a service endpoint.
   *
   * @param address
   *   The endpoint address.
   * @param node
   *   The service node.
   * @return
   *   A new service endpoint instance.
   */
  public ServiceEndpoint createServiceEndpoint(String address, Node node) {
    return new DefaultServiceEndpoint(address, node, vertx);
  }

}
