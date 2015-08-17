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

import net.kuujo.copycat.manager.PathChildren;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Copycat node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Node {
  private final String path;
  private final Copycat copycat;

  public Node(String path, Copycat copycat) {
    if (path == null)
      throw new NullPointerException("path cannot be null");
    if (copycat == null)
      throw new NullPointerException("copycat cannot be null");
    this.path = path;
    this.copycat = copycat;
  }

  /**
   * Returns this node getPath.
   *
   * @return This node getPath.
   */
  public String path() {
    return path;
  }

  /**
   * Returns a child of this node.
   * <p>
   * The returned node represents the node at the given {@code path} relative to this node's {@link #path()}. The node
   * may or may not already exist. This method does not create the returned node. In order to create the node in the
   * cluster, the user must call the {@link net.kuujo.copycat.Node#create()} method on the returned
   * {@link net.kuujo.copycat.Node}.
   *
   * @param path The child node's getPath.
   * @return The child node.
   */
  public Node node(String path) {
    if (path == null)
      throw new NullPointerException("path cannot be null");
    if (!path.startsWith(Copycat.PATH_SEPARATOR))
      path = Copycat.PATH_SEPARATOR + path;
    if (path.endsWith(Copycat.PATH_SEPARATOR))
      path = path.substring(0, path.length() - 1);
    return copycat.node(this.path + path);
  }

  /**
   * Reads a list of children for this node.
   *
   * @return A list of children for this node.
   */
  public CompletableFuture<List<Node>> children() {
    return copycat.client.submit(PathChildren.builder()
      .withPath(path)
      .build())
      .thenApply(children -> children.stream()
        .map(copycat::node)
        .collect(Collectors.toList()));
  }

  /**
   * Checks whether this node exists.
   *
   * @return Indicates whether this node exists.
   */
  public CompletableFuture<Boolean> exists() {
    return copycat.exists(path);
  }

  /**
   * Checks whether the given path exists relative to this node's {@link #path()}.
   *
   * @param path The child node's getPath.
   * @return Indicates whether the child node exists.
   */
  public CompletableFuture<Boolean> exists(String path) {
    return copycat.exists(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path));
  }

  /**
   * Creates this node.
   * <p>
   * If a node at this {@link #path()} already exists, the existing node will be returned, otherwise a new node will be
   * created. Additionally, if the node's parents don't already exist they'll be created. For instance, if this node's
   * path is {@code /foo/bar/baz} calling this method will create {@code foo}, {@code foo/bar}, and {@code foo/bar/baz}
   * if they don't already exist.
   *
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create() {
    return copycat.create(path);
  }

  /**
   * Creates a child of this node.
   * <p>
   * If a node at the given path already exists relative to this node's {@link #path()}, the existing node will be returned,
   * otherwise a new {@link net.kuujo.copycat.Node} will be returned. Additionally, if the node's parents don't already
   * exist they'll be created. For instance, calling this method with {@code /foo/bar/baz} will create {@code foo},
   * {@code foo/bar}, and {@code foo/bar/baz} if they don't already exist.
   *
   * @param path The child node's getPath.
   * @return A completable future to be completed once the child has been created.
   */
  public CompletableFuture<Node> create(String path) {
    return copycat.create(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path));
  }

  /**
   * Creates a resource at this node.
   *
   * @param type The resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed with the resource instance.
   */
  public <T extends Resource> CompletableFuture<T> create(Class<? super T> type) {
    return copycat.create(path, type);
  }

  /**
   * Creates a configurable resource at this node.
   *
   * @param type The resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed with the resource instance.
   */
  public <T extends Resource & Configurable<U>, U extends Options> CompletableFuture<T> create(Class<? super T> type, U options) {
    return copycat.create(path, type, options);
  }

  /**
   * Gets a resource at this node.
   *
   * @param type The resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed with the resource instance.
   */
  public <T extends Resource> CompletableFuture<T> get(Class<T> type) {
    return copycat.create(path, type);
  }

  /**
   * Deletes this node.
   * <p>
   * Both the {@link net.kuujo.copycat.Node} at this path and any {@link net.kuujo.copycat.Resource} associated
   * with the node will be permanently deleted, and state stored at the node will not be recoverable.
   *
   * @return A completable future to be completed once the resource has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return copycat.delete(path).thenApply(result -> null);
  }

  /**
   * Deletes a child of this node.
   *
   * @param path The child node's getPath.
   * @return A completable future to be completed once the child node has been deleted.
   */
  public CompletableFuture<Void> delete(String path) {
    return copycat.delete(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path)).thenApply(result -> null);
  }

}
