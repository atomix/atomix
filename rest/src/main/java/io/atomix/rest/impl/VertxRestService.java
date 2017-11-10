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
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitives.PrimitiveService;
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

  private final String host;
  private final int port;
  private final Vertx vertx;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ClusterEventService eventService;
  private final PrimitiveService primitiveService;
  private final PrimitiveCache primitiveCache;
  private final EventManager eventManager = new EventManager();
  private HttpServer server;
  private VertxResteasyDeployment deployment;
  private final AtomicBoolean open = new AtomicBoolean();

  public VertxRestService(
      String host,
      int port,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      PrimitiveService primitiveService) {
    this.host = host;
    this.port = port;
    this.vertx = Vertx.vertx();
    this.clusterService = checkNotNull(clusterService);
    this.communicationService = checkNotNull(communicationService);
    this.eventService = checkNotNull(eventService);
    this.primitiveService = checkNotNull(primitiveService);
    this.primitiveCache = new PrimitiveCache(primitiveService, PRIMITIVE_CACHE_SIZE);
  }

  @Override
  public CompletableFuture<RestService> open() {
    server = vertx.createHttpServer();
    deployment = new VertxResteasyDeployment();
    deployment.start();

    deployment.getDispatcher().getDefaultContextObjects().put(ClusterService.class, clusterService);
    deployment.getDispatcher().getDefaultContextObjects().put(ClusterCommunicationService.class, communicationService);
    deployment.getDispatcher().getDefaultContextObjects().put(ClusterEventService.class, eventService);
    deployment.getDispatcher().getDefaultContextObjects().put(PrimitiveService.class, primitiveService);
    deployment.getDispatcher().getDefaultContextObjects().put(PrimitiveCache.class, primitiveCache);
    deployment.getDispatcher().getDefaultContextObjects().put(EventManager.class, eventManager);

    deployment.getRegistry().addPerInstanceResource(ClusterResource.class);
    deployment.getRegistry().addPerInstanceResource(EventsResource.class);
    deployment.getRegistry().addPerInstanceResource(MessagesResource.class);
    deployment.getRegistry().addPerInstanceResource(PrimitivesResource.class);

    server.requestHandler(new VertxRequestHandler(vertx, deployment));

    CompletableFuture<RestService> future = new CompletableFuture<>();
    server.listen(port, host, result -> {
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
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
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

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
