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

import io.atomix.core.Atomix;
import io.atomix.rest.AtomixResource;

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

/**
 * Benchmark resource.
 */
@AtomixResource
@Path("/bench")
public class BenchmarkResource {
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  public Response startTest(BenchmarkConfig config, @Context Atomix atomix) {
    try {
      String testId = atomix.getCommunicationService().<BenchmarkConfig, String>send(
          BenchmarkConstants.START_SUBJECT,
          config,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          atomix.getMembershipService().getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(testId).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{testId}/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@PathParam("testId") String testId, @Context Atomix atomix) {
    try {
      BenchmarkProgress progress = atomix.getCommunicationService().<Void, BenchmarkProgress>send(
          BenchmarkConstants.PROGRESS_SUBJECT,
          null,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          atomix.getMembershipService().getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(progress).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{testId}/result")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResult(@PathParam("testId") String testId, @Context Atomix atomix) {
    try {
      BenchmarkResult result = atomix.getCommunicationService().<Void, BenchmarkResult>send(
          BenchmarkConstants.RESULT_SUBJECT,
          null,
          BenchmarkSerializer.INSTANCE::encode,
          BenchmarkSerializer.INSTANCE::decode,
          atomix.getMembershipService().getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok(result).build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return Response.serverError().build();
    }
  }

  @DELETE
  @Path("/{testId}")
  public Response stopTest(@PathParam("testId") String testId, @Context Atomix atomix) {
    try {
      atomix.getCommunicationService().send(
          BenchmarkConstants.STOP_SUBJECT,
          null,
          atomix.getMembershipService().getLocalMember().id())
          .get(10, TimeUnit.SECONDS);
      return Response.ok().build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return Response.serverError().build();
    }
  }
}
