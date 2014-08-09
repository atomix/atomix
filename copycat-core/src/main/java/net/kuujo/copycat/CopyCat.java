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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.impl.DefaultEndpointFactory;

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
    this.endpoint.init(context);
  }

  /**
   * Starts the replica.
   *
   * @return The copycat instance.
   */
  public CopyCat start() {
    return start(null);
  }

  /**
   * Starts the replica.
   *
   * @param callback A callback to be called once the replica has been started.
   * @return The copycat instance.
   */
  public CopyCat start(final AsyncCallback<Void> callback) {
    context.start(new AsyncCallback<String>() {
      @Override
      public void call(AsyncResult<String> result) {
        if (result.succeeded()) {
          callback.call(new AsyncResult<Void>((Void) null));
        } else {
          callback.call(new AsyncResult<Void>(result.cause()));
        }
      }
    });
    return this;
  }

  /**
   * Stops the replica.
   */
  public void stop() {
    stop(null);
  }

  /**
   * Stops the replica.
   *
   * @param callback A callback to be called once the replica has been stopped.
   */
  public void stop(final AsyncCallback<Void> callback) {
    endpoint.stop(callback);
  }

}
