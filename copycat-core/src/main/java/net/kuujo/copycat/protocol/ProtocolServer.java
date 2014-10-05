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
package net.kuujo.copycat.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolServer {

  /**
   * Registers a server request handler.
   *
   * @param handler A request handler to handle requests received by the server.
   */
  void requestHandler(RequestHandler handler);

  /**
   * Starts the server.
   *
   * @return A callback to be called once complete.
   */
  CompletableFuture<Void> start();

  /**
   * Starts the server.
   *
   * @return A callback to be called once complete.
   */
  CompletableFuture<Void> stop();

}
