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
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.stream.Collectors;

/**
 * Cluster resource.
 */
@Path("/cluster")
public class ClusterResource extends AbstractRestResource {

  @GET
  @Path("/node")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNode(@Context ClusterService clusterService) {
    return Response.ok(new NodeInfo(clusterService.localNode())).build();
  }

  @GET
  @Path("/nodes")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNodes(@Context ClusterService clusterService) {
    return Response.ok(clusterService.nodes().stream().map(NodeInfo::new).collect(Collectors.toList())).build();
  }

  @GET
  @Path("/nodes/{node}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNodeInfo(@PathParam("node") String nodeId, @Context ClusterService clusterService) {
    Node node = clusterService.node(NodeId.from(nodeId));
    if (node == null) {
      return Response.status(Status.NOT_FOUND).build();
    }
    return Response.ok(new NodeInfo(node)).build();
  }

  /**
   * Node information.
   */
  static class NodeInfo {
    private final Node node;

    NodeInfo(Node node) {
      this.node = node;
    }

    public String getId() {
      return node.id().id();
    }

    public String getHost() {
      return node.endpoint().host().getHostAddress();
    }

    public int getPort() {
      return node.endpoint().port();
    }

    public Node.Type getType() {
      return node.type();
    }

    public Node.State getStatus() {
      return node.state();
    }
  }
}
