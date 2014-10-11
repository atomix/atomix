/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.spi.protocol;

import net.kuujo.copycat.protocol.AsyncRequestHandler;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncProtocolClient extends AsyncRequestHandler {

  /**
   * Connects the client.
   *
   * @return A completable future to be completed once the client has connected.
   */
  CompletableFuture<Void> connect();

  /**
   * Closes the client.
   *
   * @return A completable future to be completed once the client has disconnected.
   */
  CompletableFuture<Void> close();

}
