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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.resource.*;
import net.kuujo.copycat.resource.manager.*;
import net.kuujo.copycat.util.Managed;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat implements Managed<Copycat> {
  static final String PATH_SEPARATOR = "/";
  protected final CommitLog log;
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private final ResourceRegistry registry;
  private final ResourceFactory factory = new ResourceFactory();

  public Copycat(Protocol protocol, Object... resources) {
    this(new SystemLog(new ResourceManager(), protocol), resources);
  }

  private Copycat(CommitLog log, Object... resources) {
    this.log = log;
    this.registry = new ResourceRegistry(resources);
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  public Cluster cluster() {
    return log.cluster();
  }

  /**
   * Returns a reference to the node at the given getPath.
   *
   * @param path The getPath for which to return the node.
   * @return A reference to the node at the given getPath.
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
   * Checks whether a getPath exists.
   *
   * @param path The getPath to check.
   * @return A completable future indicating whether the given getPath exists.
   */
  public CompletableFuture<Boolean> exists(String path) {
    return log.submit(new PathExists(path));
  }

  /**
   * Creates a node at the given getPath.
   *
   * @param path The getPath for which to create the node.
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create(String path) {
    return log.submit(new CreatePath(path)).thenApply(result -> node(path));
  }

  /**
   * Creates a resource at the given getPath.
   *
   * @param path The getPath at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   */
  public <T extends Resource<?>> CompletableFuture<T> create(String path, Class<T> type) {
    return log.submit(new CreateResource(path, registry.lookup(type))).thenApply(id -> factory.createResource(type, id));
  }

  /**
   * Deletes a node at the given getPath.
   *
   * @param path The getPath at which to delete the node.
   * @return A completable future to be completed once the node has been deleted.
   */
  public CompletableFuture<Copycat> delete(String path) {
    return log.submit(new DeletePath(path)).thenApply(result -> this);
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return log.open().thenApply(l -> this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return log.close();
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
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
    private <T extends Resource<?>> T createResource(Class<T> type, long id) {
      if (type.isInterface()) {
        return createResourceProxy(type, id);
      } else {
        return createResourceObject(type, id);
      }
    }

    /**
     * Creates a resource object.
     */
    @SuppressWarnings("unchecked")
    private <T extends Resource<?>> T createResourceObject(Class<T> type, long id) {
      try {
        Constructor constructor = type.getConstructor(CommitLog.class);
        return (T) constructor.newInstance(new ResourceLog(id, log));
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ResourceException("failed to instantiate resource: " + type, e);
      }
    }

    /**
     * Creates a resource proxy.
     */
    @SuppressWarnings("unchecked")
    private <T extends Resource<?>> T createResourceProxy(Class<T> type, long id) {
      return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, new ResourceProxy(type, new ResourceLog(id, log)));
    }
  }

}
