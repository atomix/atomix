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

import io.atomix.core.PrimitivesService;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.generator.AtomicIdGeneratorType;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.queue.WorkQueueType;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.tree.DocumentTreeType;
import io.atomix.core.value.AtomicValueType;
import io.atomix.primitive.PrimitiveInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.stream.Collectors;

/**
 * Primitives resource.
 */
@Path("/v1/primitives")
public class PrimitivesResource extends AbstractRestResource {

  /**
   * Returns a counter resource by name.
   */
  @Path("/counters/{name}")
  public AtomicCounterResource getCounter(@PathParam("name") String counterName, @Context PrimitivesService primitives) {
    return new AtomicCounterResource(primitives.getAtomicCounter(counterName).async());
  }

  /**
   * Gets a set of counter names.
   */
  @GET
  @Path("/counters")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCounterNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(AtomicCounterType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a leader election resource by name.
   */
  @Path("/elections/{name}")
  public LeaderElectorResource getElection(@PathParam("name") String electionName, @Context PrimitivesService primitives) {
    return new LeaderElectorResource(primitives.<String>getLeaderElection(electionName).async());
  }

  /**
   * Gets a set of election names.
   */
  @GET
  @Path("/elections")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getElectionsNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(LeaderElectorType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns an ID generator resource by name.
   */
  @Path("/ids/{name}")
  public AtomicIdGeneratorResource getIdGenerator(@PathParam("name") String generatorName, @Context PrimitivesService primitives) {
    return new AtomicIdGeneratorResource(primitives.getAtomicIdGenerator(generatorName).async());
  }

  /**
   * Gets a set of ID generator names.
   */
  @GET
  @Path("/ids")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getIdGeneratorNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(AtomicIdGeneratorType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a lock resource by name.
   */
  @Path("/locks/{name}")
  public DistributedLockResource getLock(@PathParam("name") String lockName, @Context PrimitivesService primitives) {
    return new DistributedLockResource(primitives.getLock(lockName).async());
  }

  /**
   * Gets a set of lock names.
   */
  @GET
  @Path("/locks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLockNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(DistributedLockType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a map resource by name.
   */
  @Path("/maps/{name}")
  public ConsistentMapResource getMap(@PathParam("name") String mapName, @Context PrimitivesService primitives) {
    return new ConsistentMapResource(primitives.<String, String>getConsistentMap(mapName).async());
  }

  /**
   * Gets a set of map names.
   */
  @GET
  @Path("/maps")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMapNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(ConsistentMapType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a work queue resource by name.
   */
  @Path("/queues/{name}")
  public WorkQueueResource getQueue(@PathParam("name") String queueName, @Context PrimitivesService primitives) {
    return new WorkQueueResource(primitives.<String>getWorkQueue(queueName).async());
  }

  /**
   * Gets a set of queue names.
   */
  @GET
  @Path("/queues")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueueNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(WorkQueueType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a set resource by name.
   */
  @Path("/sets/{name}")
  public DistributedSetResource getSet(@PathParam("name") String setName, @Context PrimitivesService primitives) {
    return new DistributedSetResource(primitives.<String>getSet(setName).async());
  }

  /**
   * Gets a set of map names.
   */
  @GET
  @Path("/sets")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSetNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(DistributedSetType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a document tree resource by name.
   */
  @Path("/trees/{name}")
  public DocumentTreeResource getTree(@PathParam("name") String treeName, @Context PrimitivesService primitives) {
    return new DocumentTreeResource(primitives.<String>getDocumentTree(treeName).async());
  }

  /**
   * Gets a set of tree names.
   */
  @GET
  @Path("/trees")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTreeNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(DocumentTreeType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }

  /**
   * Returns a value by name.
   */
  @Path("/values/{name}")
  public AtomicValueResource getValue(@PathParam("name") String valueName, @Context PrimitivesService primitives) {
    return new AtomicValueResource(primitives.<String>getAtomicValue(valueName).async());
  }

  /**
   * Gets a set of value names.
   */
  @GET
  @Path("/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValueNames(@Context PrimitivesService primitives) {
    return Response.ok(primitives.getPrimitives(AtomicValueType.instance()).stream().map(PrimitiveInfo::name).collect(Collectors.toSet())).build();
  }
}
