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

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.CopyCatContext;

/**
 * CopyCat endpoint.<p>
 *
 * Endpoints are user-facing interfaces that can be used to
 * send commands to a CopyCat cluster. When an endpoint receives
 * a request, if the current node is the cluster leader then the
 * command will be submitted to the cluster. If the current node
 * is not the cluster leader then the leader will be returned
 * to the client. This allows clients to locate the cluster leader.<p>
 *
 * Endpoints are defined via the CopyCat services API. To define a
 * new endpoint service, create a file in
 * <code>META-INF/services/net/kuujo/copycat/endpoints</code>. The
 * file name is the service name, and the file should contain the
 * name of a class that implements <code>Endpoint</code>.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Endpoint {

  /**
   * Initializes the endpoint.
   *
   * @param context The copycat context.
   */
  void init(CopyCatContext context);

  /**
   * Starts the endpoint.<p>
   *
   * CopyCat makes no assumptions about whether an endpoint is
   * asynchronous or not, so endpoints can be either synchronous
   * or asynchronous. Both types of endpoints should call the
   * given callback once started.
   *
   * @return A completable future to be completed once the service has been started.
   */
  CompletableFuture<Void> start();

  /**
   * Stops the endpoint.
   *
   * @return A completable future to be completed once the service has been stopped.
   */
  CompletableFuture<Void> stop();

}
