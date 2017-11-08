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

/**
 * Primitives resource.
 */
@Path("/primitives")
public class PrimitivesResource extends AbstractRestResource {
  private final PrimitiveCache primitiveCache;

  public PrimitivesResource(PrimitiveCache primitiveCache) {
    this.primitiveCache = primitiveCache;
  }

  /**
   * Returns a counter resource by name.
   */
  @Path("/counter/{name}")
  public AtomicCounterResource getCounter(@PathParam("name") String counterName) {
    return new AtomicCounterResource(primitiveCache.getPrimitive(counterName, primitives ->
        primitives.newAtomicCounterBuilder()
            .withName(counterName)
            .buildAsync()));
  }

  /**
   * Returns a map resource by name.
   */
  @Path("/map/{name}")
  public ConsistentMapResource getMap(@PathParam("name") String mapName) {
    return new ConsistentMapResource(primitiveCache.getPrimitive(mapName, primitives ->
        primitives.<String, String>newConsistentMapBuilder()
            .withName(mapName)
            .buildAsync()));
  }

  /**
   * Returns a value by name.
   */
  @Path("/value/{name}")
  public AtomicValueResource getValue(@PathParam("name") String valueName) {
    return new AtomicValueResource(primitiveCache.getPrimitive(valueName, primitives ->
        primitives.<String>newAtomicValueBuilder()
            .withName(valueName)
            .buildAsync()));
  }
}
