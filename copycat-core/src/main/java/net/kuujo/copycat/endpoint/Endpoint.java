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
package net.kuujo.copycat.endpoint;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * CopyCat endpoint.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Endpoint {

  /**
   * Initializes the endpoint.
   *
   * @param address The endpoint address.
   * @param context The copycat context.
   */
  void init(String address, CopyCatContext context);

  /**
   * Starts the endpoint.
   *
   * @param callback An asynchronous callback to be called once the endpoint has
   *        been started.
   */
  void start(AsyncCallback<Void> callback);

  /**
   * Stops the endpoint.
   *
   * @param callback An asynchronous callback to be called once the endpoint has
   *        been stopped.
   */
  void stop(AsyncCallback<Void> callback);

}
