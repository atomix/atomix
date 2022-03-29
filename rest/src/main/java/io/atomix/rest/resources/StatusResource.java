// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.rest.AtomixResource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * Status resource.
 */
@AtomixResource
@Path("/status")
public class StatusResource {
  @GET
  public Response getStatus() {
    return Response.ok().build();
  }
}
