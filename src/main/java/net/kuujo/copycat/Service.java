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
 * Base interface for types that can be asynchronously started and stopped.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Service {

  /**
   * Starts the service.
   *
   * @return The service.
   */
  Service start();

  /**
   * Starts the service.
   *
   * @param doneHandler An asynchronous handler to be called once the service has been started.
   * @return The service.
   */
  Service start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the service.
   */
  void stop();

  /**
   * Stops the service.
   *
   * @param doneHandler An asynchronous handler to be called once the service has been stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
