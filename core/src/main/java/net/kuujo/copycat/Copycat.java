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
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.manager.*;
import net.kuujo.copycat.raft.Protocol;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.util.Managed;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat implements Managed<Copycat> {

  /**
   * Returns a new Copycat builder.
   *
   * @return A new Copycat builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  static final String PATH_SEPARATOR = "/";
  protected final Raft raft;
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private final ResourceRegistry registry;
  private final ResourceFactory factory = new ResourceFactory();
  private final Map<String, Set<EventListener>> listeners = new ConcurrentHashMap<>();

  private Copycat(Raft raft, ClassLoader classLoader) {
    this.raft = raft;
    this.registry = new ResourceRegistry(classLoader);
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  public Cluster cluster() {
    return raft.cluster();
  }

  /**
   * Handles an event.
   */
  private CompletableFuture<Boolean> handleEvent(Event event) {
    Set<EventListener> listeners = this.listeners.get(event.path());
    if (listeners != null) {
      listeners.forEach(l -> l.accept(event));
    }
    return CompletableFuture.completedFuture(true);
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
    return raft.submit(new PathExists(path));
  }

  /**
   * Creates a node at the given getPath.
   *
   * @param path The getPath for which to create the node.
   * @return A completable future to be completed once the node has been created.
   */
  public CompletableFuture<Node> create(String path) {
    return raft.submit(CreatePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> node(path));
  }

  /**
   * Creates a resource at the given getPath.
   *
   * @param path The getPath at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   */
  public <T extends Resource> CompletableFuture<T> create(String path, Class<? super T> type) {
    return raft.submit(CreateResource.builder()
      .withPath(path)
      .withType(registry.lookup(type))
      .build())
      .thenApply(id -> factory.createResource(type, id));
  }

  /**
   * Deletes a node at the given getPath.
   *
   * @param path The getPath at which to delete the node.
   * @return A completable future to be completed once the node has been deleted.
   */
  public CompletableFuture<Copycat> delete(String path) {
    return raft.submit(DeletePath.builder()
      .withPath(path)
      .build())
      .thenApply(result -> this);
  }

  /**
   * Listens for state changes at the given path.
   *
   * @param path The path to which to listen for changes.
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been registered.
   */
  public CompletableFuture<Copycat> listen(String path, EventListener listener) {
    return raft.submit(AddListener.builder()
      .withPath(path)
      .build())
      .thenApply(result -> {
        Set<EventListener> listeners = this.listeners.computeIfAbsent(path, p -> new ConcurrentSkipListSet<>());
        listeners.add(listener);
        return this;
      });
  }

  /**
   * Unlistens for state changes at the given path.
   *
   * @param path The path from which to stop listening for changes.
   * @param listener The change listener.
   * @return A completable future to be called once the listener has been unregistered.
   */
  public CompletableFuture<Copycat> unlisten(String path, EventListener listener) {
    return raft.submit(RemoveListener.builder()
      .withPath(path)
      .build())
      .thenApply(result -> {
        Set<EventListener> listeners = this.listeners.get(path);
        if (listeners != null) {
          listeners.remove(listener);
          if (listeners.isEmpty()) {
            this.listeners.remove(path);
          }
        }
        return this;
      });
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return raft.open().thenApply(l -> {
      raft.cluster().member().registerHandler(Event.TOPIC, this::handleEvent);
      return this;
    });
  }

  @Override
  public boolean isOpen() {
    return raft.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    raft.cluster().member().unregisterHandler(Event.TOPIC);
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
        Constructor constructor = type.getConstructor(Protocol.class);
        return (T) constructor.newInstance(new ResourceProtocol(id, raft));
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ResourceException("failed to instantiate resource: " + type, e);
      }
    }
  }

  /**
   * Copycat builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Copycat> {
    private final Raft.Builder builder = Raft.builder();
    private Cluster cluster;
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Builder() {
    }

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(ManagedCluster cluster) {
      this.cluster = cluster;
      builder.withCluster(cluster);
      return this;
    }

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(Log log) {
      builder.withLog(log);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout) {
      builder.withElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout, TimeUnit unit) {
      builder.withElectionTimeout(electionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval) {
      builder.withHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval.
     * @param unit The heartbeat interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
      builder.withHeartbeatInterval(heartbeatInterval, unit);
      return this;
    }

    /**
     * Sets the Copycat class loader.
     *
     * @param classLoader The Copycat class loader.
     * @return The Copycat builder.
     */
    public Builder withClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @Override
    public Copycat build() {
      return new Copycat(builder.withStateMachine(new ResourceManager(cluster)).build(), classLoader);
    }
  }

}
