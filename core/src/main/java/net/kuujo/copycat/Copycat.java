/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat;

import net.kuujo.copycat.manager.CreatePath;
import net.kuujo.copycat.manager.CreateResource;
import net.kuujo.copycat.manager.DeletePath;
import net.kuujo.copycat.manager.PathExists;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.RaftClient;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Managed;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base type for creating and managing distributed {@link net.kuujo.copycat.Resource resources} in a Copycat cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link net.kuujo.copycat.Resource} objects via a filesystem like interface. There is a
 * one-to-one relationship between paths and resources, so each path can be associated with one and only one resource.
 * <p>
 * To create a resource, create a {@link net.kuujo.copycat.Node} and then create the resource by passing the resource
 * {@link java.lang.Class} to the {@link Node#create(Class)} method. When a resource is created, the
 * {@link StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 * <p>
 * {@link net.kuujo.copycat.Resource} implementations serve as a user-friendly interface through which to submit
 * {@link Command commands} and {@link Query queries} to the underlying
 * {@link RaftClient} client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Copycat implements Managed<Copycat> {
  static final String PATH_SEPARATOR = "/";
  protected final RaftClient client;
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private final Map<Long, ResourceContext> resources = new ConcurrentHashMap<>();

  protected Copycat(RaftClient client) {
    this.client = client;
  }

  /**
   * Returns a reference to the node at the given path.
   * <p>
   * The returned node represents the node at the given {@code path}. The node may or may not already exist.
   * This method does not create the returned node. In order to create the node in the cluster, the user must call
   * the {@link net.kuujo.copycat.Node#create()} method on the returned {@link net.kuujo.copycat.Node} or alternatively
   * call {@link #create(String)} directly.
   *
   * @param path The path for which to return the node.
   * @return A reference to the node at the given path.
   */
  public Node node(String path) {
    if (path == null)
      throw new NullPointerException("path cannot be null");
    if (!path.startsWith(PATH_SEPARATOR))
      path = PATH_SEPARATOR + path;
    if (path.endsWith(PATH_SEPARATOR))
      path = path.substring(0, path.length() - 1);
    return nodes.computeIfAbsent(path, p -> new Node(p, this));
  }

  /**
   * Checks whether a path exists.
   *
   * @param path The path to check.
   * @return A completable future indicating whether the given path exists.
   */
  public CompletableFuture<Boolean> exists(String path) {
    return client.submit(new PathExists(path));
  }

  /**
   * Creates a node at the given path.
   * <p>
   * If a node at the given path already exists, the existing node will be returned, otherwise a new {@link net.kuujo.copycat.Node}
   * will be returned. Additionally, if the node's parents don't already exist they'll be created. For instance, calling
   * this method with {@code /foo/bar/baz} will create {@code foo}, {@code foo/bar}, and {@code foo/bar/baz} if they
   * don't already exist.
   *
   * @param path The path for which to create the node.
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create(String path) {
    return client.submit(CreatePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> node(path));
  }

  /**
   * Creates a resource at the given path.
   * <p>
   * If a node at the given path already exists, the existing node will be returned, otherwise a new {@link net.kuujo.copycat.Node}
   * will be returned. Additionally, if the node's parents don't already exist they'll be created. For instance, calling
   * this method with {@code /foo/bar/baz} will create {@code foo}, {@code foo/bar}, and {@code foo/bar/baz} if they
   * don't already exist.
   *
   * @param path The path at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> create(String path, Class<? super T> type) {
    try {
      T resource = (T) type.newInstance();
      return client.submit(CreateResource.builder()
        .withPath(path)
        .withType(resource.stateMachine())
        .build())
        .thenApply(id -> {
          resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, client)));
          return resource;
        });
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ResourceException("failed to instantiate resource: " + type, e);
    }
  }

  /**
   * Deletes a node at the given path.
   * <p>
   * Both the {@link net.kuujo.copycat.Node} at the given path and any {@link net.kuujo.copycat.Resource} associated
   * with the node will be permanently deleted, and state stored at the node will not be recoverable.
   *
   * @param path The path at which to delete the node.
   * @return A completable future to be completed once the node has been deleted.
   */
  public CompletableFuture<Copycat> delete(String path) {
    return client.submit(DeletePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Copycat> open() {
    return client.open().thenApply(v -> this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[session=%s]", getClass().getSimpleName(), client.session());
  }

  /**
   * Copycat builder.
   */
  public static abstract class Builder<T extends Copycat> extends net.kuujo.copycat.util.Builder<T> {
  }

}
