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

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

/**
 * Primitives resource.
 */
@Path("/primitives")
public class PrimitivesResource extends AbstractRestResource {

  /**
   * Returns a counter resource by name.
   */
  @Path("/counter/{name}")
  public AtomicCounterResource getCounter(@PathParam("name") String counterName, @Context PrimitiveCache primitiveCache) {
    return new AtomicCounterResource(primitiveCache.getPrimitive(counterName, primitives ->
        primitives.newAtomicCounterBuilder()
            .withName(counterName)
            .buildAsync()));
  }

  /**
   * Returns a leader election resource by name.
   */
  @Path("/election/{name}")
  public LeaderElectorResource getElection(@PathParam("name") String electionName, @Context PrimitiveCache primitiveCache) {
    return new LeaderElectorResource(primitiveCache.getPrimitive(electionName, primitives ->
        primitives.<String>newLeaderElectorBuilder()
            .withName(electionName)
            .buildAsync()));
  }

  /**
   * Returns a lock resource by name.
   */
  @Path("/lock/{name}")
  public DistributedLockResource getLock(@PathParam("name") String lockName, @Context PrimitiveCache primitiveCache) {
    return new DistributedLockResource(primitiveCache.getPrimitive(lockName, primitives ->
        primitives.newLockBuilder()
            .withName(lockName)
            .buildAsync()));
  }

  /**
   * Returns a map resource by name.
   */
  @Path("/map/{name}")
  public ConsistentMapResource getMap(@PathParam("name") String mapName, @Context PrimitiveCache primitiveCache) {
    return new ConsistentMapResource(primitiveCache.getPrimitive(mapName, primitives ->
        primitives.<String, String>newConsistentMapBuilder()
            .withName(mapName)
            .buildAsync()));
  }

  /**
   * Returns a work queue resource by name.
   */
  @Path("/queue/{name}")
  public WorkQueueResource getQueue(@PathParam("name") String queueName, @Context PrimitiveCache primitiveCache) {
    return new WorkQueueResource(primitiveCache.getPrimitive(queueName, primitives ->
        primitives.<String>newWorkQueueBuilder()
            .withName(queueName)
            .buildAsync()));
  }

  /**
   * Returns a value by name.
   */
  @Path("/value/{name}")
  public AtomicValueResource getValue(@PathParam("name") String valueName, @Context PrimitiveCache primitiveCache) {
    return new AtomicValueResource(primitiveCache.getPrimitive(valueName, primitives ->
        primitives.<String>newAtomicValueBuilder()
            .withName(valueName)
            .buildAsync()));
  }
}
