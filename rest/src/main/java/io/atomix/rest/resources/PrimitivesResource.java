// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import com.google.common.collect.Maps;
import io.atomix.core.PrimitivesService;
import io.atomix.rest.AtomixResource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Primitives resource.
 */
@AtomixResource
@Path("/primitives")
public class PrimitivesResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPrimitives(@Context PrimitivesService primitives) {
    Map<String, PrimitiveInfo> primitivesInfo = primitives.getPrimitives()
        .stream()
        .map(info -> Maps.immutableEntry(info.name(), new PrimitiveInfo(info.name(), info.type().name())))
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    return Response.ok(primitivesInfo).build();
  }

  static class PrimitiveInfo {
    private String name;
    private String type;

    PrimitiveInfo(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }
}
