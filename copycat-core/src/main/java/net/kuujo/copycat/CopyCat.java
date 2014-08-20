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

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.impl.DefaultEndpointFactory;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.registry.Registry;

/**
 * Primary copycat API.<p>
 *
 * The <code>CopyCat</code> class provides a fluent API for
 * combining the {@link CopyCatContext} with an {@link Endpoint}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCat {
  private final Endpoint endpoint;
  private final CopyCatContext context;

  public CopyCat(Endpoint endpoint, StateMachine stateMachine, ClusterConfig cluster) {
    this.endpoint = endpoint;
    this.context = new CopyCatContext(stateMachine, cluster);
  }

  public CopyCat(String uri, StateMachine stateMachine, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster, config);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.context = new CopyCatContext(stateMachine, logFactory, cluster, config, registry);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  /**
   * Starts the replica.
   *
   * @return A completable future to be completed once the replica has started.
   */
  public CompletableFuture<Void> start() {
    return context.start().thenRun(()->{});
  }

  /**
   * Stops the replica.
   *
   * @return A completable future to be completed once the replica has stopped.
   */
  public CompletableFuture<Void> stop() {
    return endpoint.stop();
  }

}
