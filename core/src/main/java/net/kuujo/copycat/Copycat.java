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
import net.kuujo.copycat.raft.Protocol;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.log.RaftLog;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceException;
import net.kuujo.copycat.resource.ResourceProtocol;
import net.kuujo.copycat.resource.ResourceRegistry;
import net.kuujo.copycat.resource.manager.*;
import net.kuujo.copycat.util.Managed;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

  private Copycat(Raft raft, Object... resources) {
    this.raft = raft;
    this.registry = new ResourceRegistry(resources);
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
  public <T extends Resource> CompletableFuture<T> create(String path, Class<T> type) {
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

  @Override
  public CompletableFuture<Copycat> open() {
    return raft.open().thenApply(l -> this);
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
    private <T extends Resource> T createResource(Class<T> type, long id) {
      return createResourceObject(type, id);
    }

    /**
     * Creates a resource object.
     */
    @SuppressWarnings("unchecked")
    private <T extends Resource> T createResourceObject(Class<T> type, long id) {
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
    private final Raft.Builder builder = Raft.builder().withTopic("copycat");
    private final List<String> resources = new ArrayList<>();

    private Builder() {
    }

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(ManagedCluster cluster) {
      builder.withCluster(cluster);
      return this;
    }

    /**
     * Sets the Raft topic.
     *
     * @param topic The Raft topic.
     * @return The Raft builder.
     */
    public Builder withTopic(String topic) {
      builder.withTopic(topic);
      return this;
    }

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(RaftLog log) {
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
     * Sets the Copycat resources.
     *
     * @param resources A collection of resources.
     * @return The Copycat builder.
     */
    public Builder withResources(String... resources) {
      this.resources.clear();
      if (resources != null) {
        this.resources.addAll(Arrays.asList(resources));
      }
      return this;
    }

    /**
     * Sets the Copycat resources.
     *
     * @param resources A collection of resources.
     * @return The Copycat builder.
     */
    public Builder withResources(Collection<String> resources) {
      this.resources.clear();
      if (resources != null) {
        this.resources.addAll(resources);
      }
      return this;
    }

    /**
     * Adds a collection of resources.
     *
     * @param resources A collection of resources.
     * @return The Copycat builder.
     */
    public Builder addResources(String... resources) {
      if (resources != null) {
        return addResources(Arrays.asList(resources));
      }
      return this;
    }

    /**
     * Adds a collection of resources.
     *
     * @param resources A collection of resources.
     * @return The Copycat builder.
     */
    public Builder addResources(Collection<String> resources) {
      this.resources.addAll(resources);
      return this;
    }

    /**
     * Adds a resource.
     *
     * @param resource The resource to add.
     * @return The Copycat builder.
     */
    public Builder addResource(String resource) {
      resources.add(resource);
      return this;
    }

    @Override
    public Copycat build() {
      return new Copycat(builder.withStateMachine(new ResourceManager()).build(), resources.toArray(new String[resources.size()]));
    }
  }

}
