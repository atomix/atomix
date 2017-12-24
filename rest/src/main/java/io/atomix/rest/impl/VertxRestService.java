/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.rest.impl;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.core.Atomix;
import io.atomix.core.PrimitivesService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.messaging.Endpoint;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.rest.resources.ClusterResource;
import io.atomix.rest.resources.EventsResource;
import io.atomix.rest.resources.MessagesResource;
import io.atomix.rest.resources.PrimitivesResource;
import io.atomix.rest.utils.EventManager;
import io.atomix.rest.utils.PrimitiveCache;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Vert.x REST service.
 */
public class VertxRestService implements ManagedRestService {
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxRestService.class);
  private static final int PRIMITIVE_CACHE_SIZE = 1000;

  private final Atomix atomix;
  private final Endpoint endpoint;
  private final Vertx vertx;
  private HttpServer server;
  private VertxResteasyDeployment deployment;
  private final AtomicBoolean open = new AtomicBoolean();

  public VertxRestService(Atomix atomix, Endpoint endpoint) {
    this.atomix = checkNotNull(atomix, "atomix cannot be null");
    this.endpoint = checkNotNull(endpoint, "endpoint cannot be null");
    this.vertx = Vertx.vertx();
  }

  @Override
  public CompletableFuture<RestService> start() {
    server = vertx.createHttpServer();
    deployment = new VertxResteasyDeployment();
    deployment.start();

    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterService.class, atomix.clusterService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterMessagingService.class, atomix.messagingService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterEventingService.class, atomix.eventingService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(PrimitivesService.class, atomix.primitivesService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(PrimitiveCache.class, new PrimitiveCache(atomix.primitivesService(), PRIMITIVE_CACHE_SIZE));
    deployment.getDispatcher().getDefaultContextObjects()
        .put(EventManager.class, new EventManager());

    deployment.getRegistry().addPerInstanceResource(ClusterResource.class);
    deployment.getRegistry().addPerInstanceResource(EventsResource.class);
    deployment.getRegistry().addPerInstanceResource(MessagesResource.class);
    deployment.getRegistry().addPerInstanceResource(PrimitivesResource.class);

    server.requestHandler(new VertxRequestHandler(vertx, deployment));

    CompletableFuture<RestService> future = new CompletableFuture<>();
    server.listen(endpoint.port(), endpoint.host().getHostAddress(), result -> {
      if (result.succeeded()) {
        open.set(true);
        LOGGER.info("Started");
        future.complete(this);
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }

  @Override
  public boolean isRunning() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (server != null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      server.close(result -> {
        LOGGER.info("Stopped");
        future.complete(null);
      });
      deployment.stop();
      return future;
    }
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Vert.x REST service builder.
   */
  public static class Builder extends RestService.Builder {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 5678;

    @Override
    public ManagedRestService build() {
      if (endpoint == null) {
        endpoint = Endpoint.from(DEFAULT_HOST, DEFAULT_PORT);
      }
      return new VertxRestService(atomix, endpoint);
    }
  }
}
