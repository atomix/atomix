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
import net.kuujo.copycat.endpoint.EndpointUri;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.ServiceInfo;

/**
 * Primary copycat API.
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

  @SuppressWarnings("unchecked")
  public CopyCat(String endpoint, StateMachine stateMachine, ClusterConfig cluster) {
    EndpointUri uri = new EndpointUri(endpoint);
    ServiceInfo info = uri.getServiceInfo();
    Class<? extends Endpoint> endpointClass = info.getProperty("class", Class.class);
    try {
      this.endpoint = endpointClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new CopyCatException(e);
    }
    this.context = new CopyCatContext(stateMachine, cluster);
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
      public void complete(String value) {
        endpoint.start(new AsyncCallback<Void>() {
          @Override
          public void complete(Void value) {
            callback.complete(null);
          }
          @Override
          public void fail(Throwable t) {
            context.stop();
            callback.fail(t);
          }
        });
      }
      @Override
      public void fail(Throwable t) {
        callback.fail(t);
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
    endpoint.stop(new AsyncCallback<Void>() {
      @Override
      public void complete(Void value) {
        context.stop(new AsyncCallback<Void>() {
          @Override
          public void complete(Void value) {
            callback.complete(null);
          }
          @Override
          public void fail(Throwable t) {
            callback.fail(t);
          }
        });
      }
      @Override
      public void fail(Throwable t) {
        callback.fail(t);
      }
    });
  }

}
