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
package net.kuujo.copycat.spi.service;

import net.kuujo.copycat.protocol.SubmitHandler;

import java.util.concurrent.CompletableFuture;

/**
 * CopyCat service.<p>
 *
 * Endpoints are user-facing interfaces that can be used to
 * send commands to a CopyCat cluster. When an service receives
 * a request, if the current node is the cluster leader then the
 * command will be submitted to the cluster. If the current node
 * is not the cluster leader then the leader will be returned
 * to the client. This allows clients to locate the cluster leader.<p>
 *
 * Endpoints are defined via the CopyCat services API. To define a
 * new service service, create a file in
 * <code>META-INF/services/net/kuujo/copycat/endpoints</code>. The
 * file name is the service name, and the file should contain the
 * name of a class that implements <code>Endpoint</code>.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Service {

  /**
   * Registers a submit handler on the service.
   *
   * @param handler A submit request handler.
   */
  void submitHandler(SubmitHandler handler);

  /**
   * Starts the service.<p>
   *
   * CopyCat makes no assumptions about whether an service is
   * asynchronous or not, so endpoints can be either synchronous
   * or asynchronous. Both types of endpoints should call the
   * given callback once started.
   *
   * @return A completable future to be completed once the service has been started.
   */
  CompletableFuture<Void> start();

  /**
   * Stops the service.
   *
   * @return A completable future to be completed once the service has been stopped.
   */
  CompletableFuture<Void> stop();

}
