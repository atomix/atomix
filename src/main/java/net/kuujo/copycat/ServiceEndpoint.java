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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A service endpoint.
 *
 * @author Jordan Halterman
 */
public interface ServiceEndpoint {

  /**
   * Sets the endpoint address.
   *
   * @param address
   *   The service endpoint address.
   * @return
   *   The service endpoint.
   */
  ServiceEndpoint setAddress(String address);

  /**
   * Returns the endpoint address.
   *
   * @return
   *   The service endpoint address.
   */
  String getAddress();

  /**
   * Starts the endpoint.
   *
   * @return
   *   The service endpoint.
   */
  ServiceEndpoint start();

  /**
   * Starts the endpoint.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the endpoint has started.
   * @return
   *   The service endpoint.
   */
  ServiceEndpoint start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the endpoint.
   */
  void stop();

  /**
   * Stops the endpoint.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the endpoint has stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
