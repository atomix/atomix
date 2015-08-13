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
package net.kuujo.copycat.manager;

import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.StateMachineExecutor;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.resource.ResourceOperation;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.ThreadPoolContext;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager extends StateMachine {
  private static final String PATH_SEPARATOR = "/";
  private final ScheduledExecutorService scheduler;
  private StateMachineExecutor executor;
  private NodeHolder node;
  private final Map<Long, NodeHolder> nodes = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final ResourceCommitPool commits = new ResourceCommitPool();

  public ResourceManager(ScheduledExecutorService scheduler) {
    if (scheduler == null)
      throw new NullPointerException("executor cannot be null");
    this.scheduler = scheduler;
  }

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
    executor.register(ResourceOperation.class, (Function<Commit<ResourceOperation>, CompletableFuture<Object>>) this::operateResource);
    executor.register(CreatePath.class, this::createPath);
    executor.register(PathExists.class, this::pathExists);
    executor.register(PathChildren.class, this::pathChildren);
    executor.register(DeletePath.class, this::deletePath);
    executor.register(CreateResource.class, this::createResource);
    executor.register(DeleteResource.class, this::deleteResource);
  }

  /**
   * Initializes the path.
   */
  private void init(Commit commit) {
    if (node == null) {
      node = new NodeHolder(PATH_SEPARATOR, PATH_SEPARATOR, commit.index(), commit.time());
    }
  }

  /**
   * Applies resource commands.
   */
  @SuppressWarnings("unchecked")
  protected CompletableFuture<Object> operateResource(Commit<? extends ResourceOperation> commit) {
    final ResourceHolder resource = resources.get(commit.operation().resource());
    if (resource != null) {
      CompletableFuture<Object> future = new ComposableFuture<>();
      ResourceCommit resourceCommit = commits.acquire(commit, resource.sessions.computeIfAbsent(commit.session().id(),
        id -> new ManagedResourceSession(commit.operation().resource(), commit.session())));
      resource.executor.execute(resourceCommit).whenComplete((BiConsumer<Object, Throwable>) (result, error) -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally(error);
        }
      });
      return future;
    }
    throw new IllegalArgumentException("unknown resource: " + commit.operation().resource());
  }

  /**
   * Applies a create commit.
   */
  protected boolean createPath(Commit<CreatePath> commit) {
    String path = commit.operation().path();

    init(commit);

    NodeHolder node = this.node;

    StringBuilder currentPath = new StringBuilder();
    boolean created = false;
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        currentPath.append("/").append(name);
        NodeHolder child = node.children.get(name);
        if (child == null) {
          child = new NodeHolder(name, currentPath.toString(), commit.index(), commit.time());
          node.children.put(child.name, child);
          created = true;
        }
        node = child;
      }
    }

    return created;
  }

  /**
   * Applies an exists commit.
   */
  protected boolean pathExists(Commit<PathExists> commit) {
    String path = commit.operation().path();

    if (this.node == null)
      return false;

    NodeHolder node = this.node;
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        node = node.children.get(name);
        if (node == null) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Applies a getPath children commit.
   */
  @SuppressWarnings("unchecked")
  protected List<String> pathChildren(Commit<PathChildren> commit) {
    String path = commit.operation().path();

    if (this.node == null)
      return Collections.EMPTY_LIST;

    NodeHolder node = this.node;
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        node = node.children.get(name);
        if (node == null) {
          return Collections.EMPTY_LIST;
        }
      }
    }

    return new ArrayList<>(node.children.keySet());
  }

  /**
   * Applies a delete commit.
   */
  protected boolean deletePath(Commit<DeletePath> commit) {
    String path = commit.operation().path();

    init(commit);

    NodeHolder parent = null;
    NodeHolder node = this.node;
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        parent = node;
        node = node.children.get(name);
        if (node == null) {
          return false;
        }
      }
    }

    if (parent != null) {
      parent.children.remove(node.name);
      return true;
    }
    return false;
  }

  /**
   * Applies a create resource commit.
   */
  protected long createResource(Commit<CreateResource> commit) {
    String path = commit.operation().path();

    init(commit);

    NodeHolder node = this.node;

    StringBuilder currentPath = new StringBuilder();
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        currentPath.append("/").append(name);
        NodeHolder child = node.children.get(name);
        if (child == null) {
          child = new NodeHolder(name, currentPath.toString(), commit.index(), commit.time());
          node.children.put(child.name, child);
        }
        node = child;
      }
    }

    if (node.resource == 0) {
      node.resource = commit.index();
      try {
        StateMachine resource = commit.operation().type().newInstance();
        nodes.put(node.resource, node);
        Context context = new ThreadPoolContext(scheduler, Context.currentContext().serializer().clone());
        resources.put(node.resource, new ResourceHolder(resource, new ResourceStateMachineExecutor(executor, context)));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    }

    return node.resource;
  }

  /**
   * Applies a delete resource commit.
   */
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    init(commit);

    NodeHolder node = nodes.remove(commit.operation().resource());
    if (node != null) {
      node.resource = 0;
    }

    ResourceHolder resource = resources.remove(commit.operation().resource());
    if (resource != null) {
      resource.executor.close();
      return true;
    }
    return false;
  }

  @Override
  public void register(Session session) {
    for (Map.Entry<Long, ResourceHolder> entry : resources.entrySet()) {
      ResourceHolder resource = entry.getValue();
      resource.executor.execute(() -> resource.stateMachine.register(resource.sessions.computeIfAbsent(session.id(), id -> new ManagedResourceSession(entry.getKey(), session))));
    }
  }

  @Override
  public void close(Session session) {
    for (ResourceHolder resource : resources.values()) {
      ManagedResourceSession resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.executor.execute(() -> resource.stateMachine.close(resourceSession));
      }
    }
  }

  @Override
  public void expire(Session session) {
    for (ResourceHolder resource : resources.values()) {
      ManagedResourceSession resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.executor.execute(() -> resource.stateMachine.expire(resourceSession));
      }
    }
  }

  @Override
  public void close() {
    scheduler.shutdown();
  }

  /**
   * Node holder.
   */
  private static class NodeHolder {
    private final String name;
    private final String path;
    private final long version;
    private final Instant created;
    private long resource;
    private final Map<String, NodeHolder> children = new LinkedHashMap<>();

    public NodeHolder(String name, String path, long version, Instant created) {
      this.name = name;
      this.path = path;
      this.version = version;
      this.created = created;
    }
  }

  /**
   * Resource holder.
   */
  private static class ResourceHolder {
    private final Map<Long, ManagedResourceSession> sessions = new HashMap<>();
    private final StateMachine stateMachine;
    private final StateMachineExecutor executor;

    private ResourceHolder(StateMachine stateMachine, StateMachineExecutor executor) {
      this.stateMachine = stateMachine;
      this.executor = executor;
    }
  }

}
