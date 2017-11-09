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
import io.atomix.cluster.messaging.ClusterEventService;

import javax.ws.rs.Path;

/**
 * Atomix resource.
 */
@Path("/")
public class AtomixResource {
  private final ClusterService clusterService;
  private final ClusterEventService eventService;
  private final PrimitiveCache primitiveCache;

  public AtomixResource(ClusterService clusterService, ClusterEventService eventService, PrimitiveCache primitiveCache) {
    this.clusterService = clusterService;
    this.eventService = eventService;
    this.primitiveCache = primitiveCache;
  }

  @Path("/cluster")
  public ClusterResource getCluster() {
    return new ClusterResource(clusterService);
  }

  @Path("/events")
  public EventsResource getEvents() {
    return new EventsResource(eventService);
  }

  @Path("/primitives")
  public PrimitivesResource getPrimitives() {
    return new PrimitivesResource(primitiveCache);
  }
}
