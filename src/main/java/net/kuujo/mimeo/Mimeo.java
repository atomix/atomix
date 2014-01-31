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

import net.kuujo.mimeo.impl.DefaultService;
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
    this.vertx = verticle.getVertx();
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

}
