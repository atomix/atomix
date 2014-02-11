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
package net.kuujo.copycat;

import net.kuujo.copycat.impl.DefaultReplica;

import org.vertx.java.core.Vertx;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

/**
 * Primary API for copycat.
 *
 * @author Jordan Halterman
 */
public final class CopyCat {
  private final Vertx vertx;
  private final Container container;

  public CopyCat(Verticle verticle) {
    this(verticle.getVertx(), verticle.getContainer());
  }

  public CopyCat(Vertx vertx, Container container) {
    this.vertx = vertx;
    this.container = container;
  }

  /**
   * Creates a new replica.
   *
   * @param address The replica address.
   * @param stateMachine The replica state machine.
   * @return The replica instance.
   */
  public Replica createReplica(String address, StateMachine stateMachine) {
    return new DefaultReplica(address, vertx, container, stateMachine);
  }

  /**
   * Creates a new replica.
   *
   * @param address The replica address.
   * @param stateMachine The replica state machine.
   * @param config The replica configuration.
   * @return The replica instance.
   */
  public Replica createReplica(String address, StateMachine stateMachine, ClusterConfig config) {
    return new DefaultReplica(address, vertx, container, stateMachine, config);
  }

}
