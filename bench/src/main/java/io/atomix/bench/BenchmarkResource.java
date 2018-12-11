/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.AtomixRegistry;
import io.atomix.rest.AtomixResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Benchmark resource.
 */
@AtomixResource
@Path("/bench")
public class BenchmarkResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkResource.class);

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  public Response startTest(
      BenchmarkConfig config,
      @Context ClusterMembershipService membershipService,
      @Context ClusterCommunicationService communicationService) {
    try {
      String testId = communicationService.<BenchmarkConfig, String>send(
          BenchmarkConstants.START_SUBJECT,
          config,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          membershipService.getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(testId).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.warn("An uncaught exception occurred", e);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/types")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTypes(@Context AtomixRegistry registry) {
    return Response.ok(registry.getTypes(BenchmarkType.class)
        .stream()
        .map(type -> type.name())
        .collect(Collectors.toList()))
        .build();
  }

  @GET
  @Path("/{testId}/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(
      @PathParam("testId") String testId,
      @Context ClusterMembershipService membershipService,
      @Context ClusterCommunicationService communicationService) {
    try {
      BenchmarkProgress progress = communicationService.<String, BenchmarkProgress>send(
          BenchmarkConstants.PROGRESS_SUBJECT,
          testId,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          membershipService.getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(progress).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.warn("An uncaught exception occurred", e);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{testId}/result")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResult(
      @PathParam("testId") String testId,
      @Context ClusterMembershipService membershipService,
      @Context ClusterCommunicationService communicationService) {
    try {
      BenchmarkResult result = communicationService.<String, BenchmarkResult>send(
          BenchmarkConstants.RESULT_SUBJECT,
          testId,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          membershipService.getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(result).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.warn("An uncaught exception occurred", e);
      return Response.serverError().build();
    }
  }

  @DELETE
  @Path("/{testId}")
  public Response stopTest(
      @PathParam("testId") String testId,
      @Context ClusterMembershipService membershipService,
      @Context ClusterCommunicationService communicationService) {
    try {
      communicationService.send(
          BenchmarkConstants.STOP_SUBJECT,
          testId,
          membershipService.getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok().build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.warn("An uncaught exception occurred", e);
      return Response.serverError().build();
    }
  }
}
