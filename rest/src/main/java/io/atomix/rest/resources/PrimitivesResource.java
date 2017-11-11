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
package io.atomix.rest.resources;

import io.atomix.primitives.PrimitiveService;
import io.atomix.rest.utils.PrimitiveCache;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Primitives resource.
 */
@Path("/v1/primitives")
public class PrimitivesResource extends AbstractRestResource {

  /**
   * Returns a counter resource by name.
   */
  @Path("/counters/{name}")
  public AtomicCounterResource getCounter(@PathParam("name") String counterName, @Context PrimitiveCache primitiveCache) {
    return new AtomicCounterResource(primitiveCache.getPrimitive(counterName, primitives ->
        primitives.atomicCounterBuilder()
            .withName(counterName)
            .buildAsync()));
  }

  /**
   * Gets a set of counter names.
   */
  @GET
  @Path("/counters")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCounterNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getAtomicCounterNames()).build();
  }

  /**
   * Returns a leader election resource by name.
   */
  @Path("/elections/{name}")
  public LeaderElectorResource getElection(@PathParam("name") String electionName, @Context PrimitiveCache primitiveCache) {
    return new LeaderElectorResource(primitiveCache.getPrimitive(electionName, primitives ->
        primitives.<String>leaderElectorBuilder()
            .withName(electionName)
            .buildAsync()));
  }

  /**
   * Gets a set of election names.
   */
  @GET
  @Path("/elections")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getElectionsNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getLeaderElectorNames()).build();
  }

  /**
   * Returns an ID generator resource by name.
   */
  @Path("/ids/{name}")
  public AtomicIdGeneratorResource getIdGenerator(@PathParam("name") String generatorName, @Context PrimitiveCache primitiveCache) {
    return new AtomicIdGeneratorResource(primitiveCache.getPrimitive(generatorName, primitives ->
        primitives.<String>atomicIdGeneratorBuilder()
            .withName(generatorName)
            .buildAsync()));
  }

  /**
   * Gets a set of ID generator names.
   */
  @GET
  @Path("/ids")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getIdGeneratorNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getAtomicIdGeneratorNames()).build();
  }

  /**
   * Returns a lock resource by name.
   */
  @Path("/locks/{name}")
  public DistributedLockResource getLock(@PathParam("name") String lockName, @Context PrimitiveCache primitiveCache) {
    return new DistributedLockResource(primitiveCache.getPrimitive(lockName, primitives ->
        primitives.lockBuilder()
            .withName(lockName)
            .buildAsync()));
  }

  /**
   * Gets a set of lock names.
   */
  @GET
  @Path("/locks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLockNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getDistributedLockNames()).build();
  }

  /**
   * Returns a map resource by name.
   */
  @Path("/maps/{name}")
  public ConsistentMapResource getMap(@PathParam("name") String mapName, @Context PrimitiveCache primitiveCache) {
    return new ConsistentMapResource(primitiveCache.getPrimitive(mapName, primitives ->
        primitives.<String, String>consistentMapBuilder()
            .withName(mapName)
            .buildAsync()));
  }

  /**
   * Gets a set of map names.
   */
  @GET
  @Path("/maps")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMapNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getConsistentMapNames()).build();
  }

  /**
   * Returns a work queue resource by name.
   */
  @Path("/queues/{name}")
  public WorkQueueResource getQueue(@PathParam("name") String queueName, @Context PrimitiveCache primitiveCache) {
    return new WorkQueueResource(primitiveCache.getPrimitive(queueName, primitives ->
        primitives.<String>workQueueBuilder()
            .withName(queueName)
            .buildAsync()));
  }

  /**
   * Gets a set of queue names.
   */
  @GET
  @Path("/queues")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueueNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getWorkQueueNames()).build();
  }

  /**
   * Returns a set resource by name.
   */
  @Path("/sets/{name}")
  public DistributedSetResource getSet(@PathParam("name") String setName, @Context PrimitiveCache primitiveCache) {
    return new DistributedSetResource(primitiveCache.getPrimitive(setName, primitives ->
        primitives.<String>setBuilder()
            .withName(setName)
            .buildAsync()));
  }

  /**
   * Gets a set of map names.
   */
  @GET
  @Path("/sets")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSetNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getSetNames()).build();
  }

  /**
   * Returns a document tree resource by name.
   */
  @Path("/trees/{name}")
  public DocumentTreeResource getTree(@PathParam("name") String treeName, @Context PrimitiveCache primitiveCache) {
    return new DocumentTreeResource(primitiveCache.getPrimitive(treeName, primitives ->
        primitives.<String>documentTreeBuilder()
            .withName(treeName)
            .buildAsync()));
  }

  /**
   * Gets a set of tree names.
   */
  @GET
  @Path("/trees")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTreeNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getDocumentTreeNames()).build();
  }

  /**
   * Returns a value by name.
   */
  @Path("/values/{name}")
  public AtomicValueResource getValue(@PathParam("name") String valueName, @Context PrimitiveCache primitiveCache) {
    return new AtomicValueResource(primitiveCache.getPrimitive(valueName, primitives ->
        primitives.<String>atomicValueBuilder()
            .withName(valueName)
            .buildAsync()));
  }

  /**
   * Gets a set of value names.
   */
  @GET
  @Path("/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValueNames(@Context PrimitiveService primitiveService) {
    return Response.ok(primitiveService.getAtomicValueNames()).build();
  }
}
