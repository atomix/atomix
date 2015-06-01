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
   * Returns a child node/
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
    return copycat.raft.submit(PathChildren.builder()
      .withPath(path)
      .build())
      .thenApply(children -> {
        return children.stream()
          .map(copycat::node)
          .collect(Collectors.toList());
      });
  }

  /**
   * Reads a boolean value indicating whether this node exists.
   *
   * @return Indicates whether this node exists.
   */
  public CompletableFuture<Boolean> exists() {
    return copycat.exists(path);
  }

  /**
   * Reads a boolean value indicating whether a child node exists.
   *
   * @param path The child node's getPath.
   * @return Indicates whether the child node exists.
   */
  public CompletableFuture<Boolean> exists(String path) {
    return copycat.exists(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path));
  }

  /**
   * Creates this node.
   *
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create() {
    return copycat.create(path);
  }

  /**
   * Creates a child of this node.
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
   * Deletes the resource at this node.
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

  /**
   * Listens for state changes at this path.
   *
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been registered.
   */
  public CompletableFuture<Node> listen(EventListener listener) {
    return copycat.listen(path, listener).thenApply(result -> this);
  }

  /**
   * Listens for state changes at the given path.
   *
   * @param path The path to which to listen for changes.
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been registered.
   */
  public CompletableFuture<Node> listen(String path, EventListener listener) {
    return copycat.listen(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path), listener).thenApply(result -> this);
  }

  /**
   * Unlistens for state changes at this path.
   *
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been unregistered.
   */
  public CompletableFuture<Node> unlisten(EventListener listener) {
    return copycat.unlisten(path, listener).thenApply(result -> this);
  }

  /**
   * Unlistens for state changes at the given path.
   *
   * @param path The path from which to stop listening for changes.
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been unregistered.
   */
  public CompletableFuture<Node> unlisten(String path, EventListener listener) {
    return copycat.unlisten(String.format("%s%s%s", this.path, Copycat.PATH_SEPARATOR, path), listener).thenApply(result -> this);
  }

}
