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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.core.Atomix;
import io.atomix.core.PrimitivesService;
import io.atomix.core.utils.EventManager;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.rest.resources.ClusterResource;
import io.atomix.rest.resources.EventsResource;
import io.atomix.rest.resources.MessagesResource;
import io.atomix.rest.resources.PrimitivesResource;
import io.atomix.rest.resources.StatusResource;
import io.atomix.utils.net.Address;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Vert.x REST service.
 */
public class VertxRestService implements ManagedRestService {
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxRestService.class);

  private final Atomix atomix;
  private final Address address;
  private final Vertx vertx;
  private HttpServer server;
  private VertxResteasyDeployment deployment;
  private final AtomicBoolean open = new AtomicBoolean();

  public VertxRestService(Atomix atomix, Address address) {
    this.atomix = checkNotNull(atomix, "atomix cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
    this.vertx = Vertx.vertx();
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public CompletableFuture<RestService> start() {
    server = vertx.createHttpServer();
    deployment = new VertxResteasyDeployment();
    deployment.start();

    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterMembershipService.class, atomix.membershipService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterCommunicationService.class, atomix.communicationService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(ClusterEventingService.class, atomix.eventingService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(PrimitivesService.class, atomix.primitivesService());
    deployment.getDispatcher().getDefaultContextObjects()
        .put(EventManager.class, new EventManager());

    deployment.getRegistry().addPerInstanceResource(StatusResource.class);
    deployment.getRegistry().addPerInstanceResource(ClusterResource.class);
    deployment.getRegistry().addPerInstanceResource(EventsResource.class);
    deployment.getRegistry().addPerInstanceResource(MessagesResource.class);
    deployment.getRegistry().addPerInstanceResource(PrimitivesResource.class);

    deployment.getDispatcher().getProviderFactory().register(new JacksonProvider(createObjectMapper()));

    server.requestHandler(new VertxRequestHandler(vertx, deployment));

    CompletableFuture<RestService> future = new CompletableFuture<>();
    server.listen(address.port(), address.address().getHostAddress(), result -> {
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

  private ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

    SimpleModule module = new SimpleModule("PolymorphicTypes");
    module.addDeserializer(PartitionGroupConfig.class, new PartitionGroupDeserializer(atomix.registryService()));
    module.addDeserializer(PrimitiveProtocolConfig.class, new PrimitiveProtocolDeserializer(atomix.registryService()));
    module.addDeserializer(PrimitiveConfig.class, new PrimitiveConfigDeserializer(atomix.registryService()));
    mapper.registerModule(module);

    return mapper;
  }

  /**
   * Vert.x REST service builder.
   */
  public static class Builder extends RestService.Builder {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 5678;

    @Override
    public ManagedRestService build() {
      if (address == null) {
        address = Address.from(DEFAULT_HOST, DEFAULT_PORT);
      }
      return new VertxRestService(atomix, address);
    }
  }

  @Provider
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  private static class JacksonProvider implements ContextResolver<ObjectMapper> {
    private final ObjectMapper mapper;

    JacksonProvider(ObjectMapper mapper) {
      this.mapper = mapper;
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
      return mapper;
    }
  }
}
