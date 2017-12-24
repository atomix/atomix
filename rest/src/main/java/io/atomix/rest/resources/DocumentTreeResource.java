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

import com.google.common.collect.Maps;

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Document tree resource.
 */
public class DocumentTreeResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentTreeResource.class);

  private final AsyncDocumentTree<String> documentTree;

  public DocumentTreeResource(AsyncDocumentTree<String> documentTree) {
    this.documentTree = documentTree;
  }

  /**
   * Returns a document path for the given path params.
   */
  private DocumentPath getDocumentPath(List<PathSegment> params) {
    if (params.isEmpty()) {
      return documentTree.root();
    } else {
      List<String> path = new ArrayList<>(documentTree.root().pathElements());
      path.addAll(params.stream().map(PathSegment::getPath).collect(Collectors.toList()));
      return DocumentPath.from(path);
    }
  }

  @GET
  @Path("/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(@PathParam("path") List<PathSegment> path, @Suspended AsyncResponse response) {
    documentTree.get(getDocumentPath(path)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(new VersionedResult(result)).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{path: .*}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public void create(@PathParam("path") List<PathSegment> path, String value, @Suspended AsyncResponse response) {
    documentTree.createRecursive(getDocumentPath(path), value).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @PUT
  @Path("/{path: .*}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public void set(@PathParam("path") List<PathSegment> path, String value, @QueryParam("version") Long version, @Suspended AsyncResponse response) {
    CompletableFuture<Boolean> future;
    if (version != null) {
      future = documentTree.replace(getDocumentPath(path), value, version);
    } else {
      future = documentTree.set(getDocumentPath(path), value).thenApply(v -> Boolean.TRUE);
    }

    future.whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        if (error.getCause() != null) {
          error = error.getCause();
        }
        if (error instanceof IllegalDocumentModificationException || error instanceof NoSuchDocumentPathException) {
          response.resume(Response.ok(false).build());
        } else {
          LOGGER.warn("{}", error);
          response.resume(Response.serverError().build());
        }
      }
    });
  }

  @GET
  @Path("/children")
  @Produces(MediaType.APPLICATION_JSON)
  public void getRootChildren(@Suspended AsyncResponse response) {
    documentTree.getChildren(documentTree.root()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(Maps.transformValues(result, VersionedResult::new)).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/children/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public void getChildren(@PathParam("path") List<PathSegment> path, @Suspended AsyncResponse response) {
    documentTree.getChildren(getDocumentPath(path)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(Maps.transformValues(result, VersionedResult::new)).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{path: .*}")
  @Produces(MediaType.APPLICATION_JSON)
  public void removeNode(@PathParam("path") List<PathSegment> path, @Suspended AsyncResponse response) {
    documentTree.removeNode(getDocumentPath(path)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(new VersionedResult(result)).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  /**
   * Versioned JSON result.
   */
  static class VersionedResult {
    private final Versioned<String> value;

    public VersionedResult(Versioned<String> value) {
      this.value = value;
    }

    public String getValue() {
      return value.value();
    }

    public long getVersion() {
      return value.version();
    }
  }
}
