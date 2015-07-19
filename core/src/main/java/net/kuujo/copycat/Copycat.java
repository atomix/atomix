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
import net.kuujo.copycat.raft.ManagedRaft;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.util.Managed;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Copycat implements Managed<Copycat> {
  static final String PATH_SEPARATOR = "/";
  protected final ManagedRaft raft;
  private final Map<Class<? extends Resource>, Class<? extends StateMachine>> typeCache = new ConcurrentHashMap<>();
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private final ResourceFactory factory = new ResourceFactory();

  protected Copycat(ManagedRaft raft) {
    this.raft = raft;
  }

  /**
   * Returns a reference to the node at the given path.
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
    return raft.submit(new PathExists(path));
  }

  /**
   * Creates a node at the given path.
   *
   * @param path The path for which to create the node.
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create(String path) {
    return raft.submit(CreatePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> node(path));
  }

  /**
   * Creates a resource at the given path.
   *
   * @param path The path at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> create(String path, Class<? super T> type) {
    Class<? extends StateMachine> stateMachine = typeCache.computeIfAbsent((Class<? extends Resource>) type, t -> {
      Stateful stateful = t.getAnnotation(Stateful.class);
      return stateful != null ? stateful.value() : null;
    });

    if (stateMachine == null) {
      throw new IllegalArgumentException("invalid resource class: " + type);
    }

    return raft.submit(CreateResource.builder()
      .withPath(path)
      .withType(stateMachine)
      .build())
      .thenApply(id -> factory.createResource(type, id));
  }

  /**
   * Deletes a node at the given path.
   *
   * @param path The path at which to delete the node.
   * @return A completable future to be completed once the node has been deleted.
   */
  public CompletableFuture<Copycat> delete(String path) {
    return raft.submit(DeletePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> this);
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return raft.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return raft.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return raft.close();
  }

  @Override
  public boolean isClosed() {
    return raft.isClosed();
  }

  /**
   * Resource factory.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  private class ResourceFactory {

    /**
     * Creates a new resource.
     */
    private <T extends Resource> T createResource(Class<? super T> type, long id) {
      return createResourceObject(type, id);
    }

    /**
     * Creates a resource object.
     */
    @SuppressWarnings("unchecked")
    private <T extends Resource> T createResourceObject(Class<? super T> type, long id) {
      try {
        Constructor constructor = type.getConstructor(Raft.class);
        return (T) constructor.newInstance(new ResourceProtocol(id, raft));
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ResourceException("failed to instantiate resource: " + type, e);
      }
    }
  }

  /**
   * Copycat builder.
   */
  public static interface Builder<T extends Copycat> extends net.kuujo.copycat.Builder<T> {
  }

}
