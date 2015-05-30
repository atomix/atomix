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
package net.kuujo.copycat.resource.manager;

import net.kuujo.copycat.cluster.Session;
import net.kuujo.copycat.raft.Apply;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.Filter;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.resource.ResourceCommand;
import net.kuujo.copycat.resource.ResourceOperation;
import net.kuujo.copycat.resource.ResourceQuery;

import java.util.*;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager extends StateMachine {
  private static final String PATH_SEPARATOR = "/";
  private NodeHolder node;
  private final Map<Long, NodeHolder> nodes = new HashMap<>();
  private final Map<Long, StateMachine> resources = new HashMap<>();

  /**
   * Initializes the path.
   */
  private void init(Commit commit) {
    if (node == null) {
      node = new NodeHolder(PATH_SEPARATOR, commit.index(), commit.timestamp());
    }
  }

  /**
   * Applies resource commands.
   */
  @SuppressWarnings("unchecked")
  @Apply({ResourceCommand.class, ResourceQuery.class})
  protected Object commandResource(Commit<? extends ResourceOperation> commit) {
    StateMachine resource = resources.get(commit.operation().resource());
    if (resource != null) {
      return resource.apply(new Commit(commit.index(), commit.session(), commit.timestamp(), commit.operation().operation()));
    }
    throw new IllegalArgumentException("unknown resource: " + commit.operation().resource());
  }

  /**
   * Filters resource commands.
   */
  @SuppressWarnings("unchecked")
  @Filter(ResourceCommand.class)
  protected boolean filterResource(Commit<ResourceCommand> commit, Compaction compaction) {
    StateMachine resource = resources.get(commit.operation().resource());
    return resource != null && resource.filter(new Commit(commit.index(), commit.session(), commit.timestamp(), commit.operation().operation()), compaction);
  }

  /**
   * Applies a create commit.
   */
  @Apply(CreatePath.class)
  protected boolean createPath(Commit<CreatePath> commit) {
    String path = commit.operation().path();

    init(commit);

    NodeHolder node = this.node;

    boolean created = false;
    for (String name : path.split(PATH_SEPARATOR)) {
      NodeHolder child = node.children.get(name);
      if (child == null) {
        child = new NodeHolder(name, commit.index(), commit.timestamp());
        node.children.put(child.name, child);
        created = true;
      }
      node = child;
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
      NodeHolder child = node.children.get(name);
      if (child == null) {
        return false;
      }
      node = child;
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
      node = node.children.get(name);
      if (node == null) {
        return false;
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
      node = node.children.get(name);
      if (node == null) {
        return Collections.EMPTY_LIST;
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
      parent = node;
      node = node.children.get(name);
      if (node == null) {
        return false;
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

    for (String name : path.split(PATH_SEPARATOR)) {
      NodeHolder child = node.children.get(name);
      if (child == null) {
        child = new NodeHolder(name, commit.index(), commit.timestamp());
        node.children.put(child.name, child);
      }
      node = child;
    }

    if (node.resource == 0) {
      node.resource = commit.index();
      try {
        StateMachine resource = commit.operation().type().newInstance();
        nodes.put(node.resource, node);
        resources.put(node.resource, resource);
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
    for (StateMachine stateMachine : resources.values()) {
      stateMachine.register(session);
    }
  }

  @Override
  public void close(Session session) {
    for (StateMachine stateMachine : resources.values()) {
      stateMachine.close(session);
    }
  }

  @Override
  public void expire(Session session) {
    for (StateMachine stateMachine : resources.values()) {
      stateMachine.expire(session);
    }
  }

  /**
   * Node holder.
   */
  private static class NodeHolder {
    private final String name;
    private final long version;
    private final long timestamp;
    private long resource;
    private final Map<String, NodeHolder> children = new LinkedHashMap<>();

    public NodeHolder(String name, long version, long timestamp) {
      this.name = name;
      this.version = version;
      this.timestamp = timestamp;
    }
  }

}
