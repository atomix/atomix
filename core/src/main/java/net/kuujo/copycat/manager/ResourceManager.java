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

import net.kuujo.copycat.ResourceCommand;
import net.kuujo.copycat.ResourceOperation;
import net.kuujo.copycat.ResourceQuery;
import net.kuujo.copycat.log.Compaction;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.ThreadPoolContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager extends StateMachine {
  private static final String PATH_SEPARATOR = "/";
  private final ScheduledExecutorService executor;
  private NodeHolder node;
  private final Map<Long, NodeHolder> nodes = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final Map<Long, Session> sessions = new HashMap<>();

  public ResourceManager(ScheduledExecutorService executor) {
    if (executor == null)
      throw new NullPointerException("executor cannot be null");
    this.executor = executor;
  }

  /**
   * Initializes the path.
   */
  private void init(Commit commit) {
    if (node == null) {
      node = new NodeHolder(PATH_SEPARATOR, PATH_SEPARATOR, commit.index(), commit.timestamp());
    }
  }

  /**
   * Applies resource commands.
   */
  @SuppressWarnings("unchecked")
  @Apply({ResourceCommand.class, ResourceQuery.class})
  protected CompletableFuture<Object> commandResource(Commit<? extends ResourceOperation> commit) {
    ResourceHolder resource = resources.get(commit.operation().resource());
    if (resource != null) {
      CompletableFuture<Object> future = new ComposableFuture<>();
      resource.context.execute(() -> {
        CompletableFuture<Object> resultFuture = resource.stateMachine.apply(new Commit(commit.index(), commit.session(), commit
          .timestamp(), commit.operation().operation()));
        resultFuture.whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
      });
      return future;
    }
    throw new IllegalArgumentException("unknown resource: " + commit.operation().resource());
  }

  /**
   * Filters resource commands.
   */
  @SuppressWarnings("unchecked")
  @Filter(ResourceCommand.class)
  protected CompletableFuture<Boolean> filterResource(Commit<ResourceCommand> commit, Compaction compaction) {
    ResourceHolder resource = resources.get(commit.operation().resource());
    if (resource == null) {
      return CompletableFuture.completedFuture(true);
    }

    CompletableFuture<Boolean> future = new CompletableFuture<>();
    resource.context.execute(() -> {
      CompletableFuture<Boolean> resultFuture = resource.stateMachine.filter(new Commit(commit.index(), commit.session(), commit
        .timestamp(), commit.operation().operation()), compaction);
      resultFuture.whenComplete((result, error) -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Applies a create commit.
   */
  @Apply(CreatePath.class)
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
          child = new NodeHolder(name, currentPath.toString(), commit.index(), commit.timestamp());
          node.children.put(child.name, child);
          created = true;
        }
        node = child;
      }
    }

    return created;
  }

  /**
   * Filters a create commit.
   */
  @Filter(CreatePath.class)
  protected boolean filterCreatePath(Commit<CreatePath> commit, Compaction compaction) {
    String path = commit.operation().path();

    init(commit);

    NodeHolder node = this.node;
    for (String name : path.split(PATH_SEPARATOR)) {
      if (!name.equals("")) {
        NodeHolder child = node.children.get(name);
        if (child == null) {
          return false;
        }
        node = child;
      }
    }
    return node.version == commit.index();
  }

  /**
   * Applies an exists commit.
   */
  @Apply(PathExists.class)
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
  @Apply(PathChildren.class)
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
  @Apply(DeletePath.class)
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
   * Filters a delete path commit.
   */
  @Filter(value=DeletePath.class, compaction=Compaction.Type.MAJOR)
  protected boolean filterDeletePath(Commit<DeletePath> commit, Compaction compaction) {
    return commit.index() >= compaction.index();
  }

  /**
   * Applies a create resource commit.
   */
  @Apply(CreateResource.class)
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
          child = new NodeHolder(name, currentPath.toString(), commit.index(), commit.timestamp());
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
        resources.put(node.resource, new ResourceHolder(resource, new ThreadPoolContext(executor, Context.currentContext().serializer().clone())));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    }

    return node.resource;
  }

  /**
   * Filters a create resource commit.
   */
  @Filter(CreateResource.class)
  protected boolean filterCreateResource(Commit<CreateResource> commit, Compaction compaction) {
    return resources.containsKey(commit.index());
  }

  /**
   * Applies a delete resource commit.
   */
  @Apply(DeleteResource.class)
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    init(commit);

    NodeHolder node = nodes.remove(commit.operation().resource());
    if (node != null) {
      node.resource = 0;
    }

    return resources.remove(commit.operation().resource()) != null;
  }

  /**
   * Filters a delete resource commit.
   */
  @Filter(value=DeleteResource.class, compaction=Compaction.Type.MAJOR)
  protected boolean filterDeleteResource(Commit<DeleteResource> commit, Compaction compaction) {
    return commit.index() >= compaction.index();
  }

  @Override
  public void register(Session session) {
    sessions.put(session.id(), session);
    for (ResourceHolder resource : resources.values()) {
      resource.context.execute(() -> resource.stateMachine.register(resource.sessions.computeIfAbsent(session.id(), id -> new ResourceSession(id, session))));
    }
  }

  @Override
  public void close(Session session) {
    sessions.remove(session.id());
    for (ResourceHolder resource : resources.values()) {
      ResourceSession resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.context.execute(() -> {
          resourceSession.close();
          resource.stateMachine.close(resourceSession);
        });
      }
    }
  }

  @Override
  public void expire(Session session) {
    sessions.remove(session.id());
    for (ResourceHolder resource : resources.values()) {
      ResourceSession resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.context.execute(() -> {
          resourceSession.expire();
          resource.stateMachine.expire(resourceSession);
        });
      }
    }
  }

  @Override
  public void close() {
    executor.shutdown();
  }

  /**
   * Node holder.
   */
  private static class NodeHolder {
    private final String name;
    private final String path;
    private final long version;
    private final long timestamp;
    private long resource;
    private final Map<String, NodeHolder> children = new LinkedHashMap<>();

    public NodeHolder(String name, String path, long version, long timestamp) {
      this.name = name;
      this.path = path;
      this.version = version;
      this.timestamp = timestamp;
    }
  }

  /**
   * Resource holder.
   */
  private static class ResourceHolder {
    private final Map<Long, ResourceSession> sessions = new HashMap<>();
    private final StateMachine stateMachine;
    private final Context context;

    private ResourceHolder(StateMachine stateMachine, Context context) {
      this.stateMachine = stateMachine;
      this.context = context;
    }
  }

}
