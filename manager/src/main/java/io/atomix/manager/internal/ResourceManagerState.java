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
package io.atomix.manager.internal;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.manager.ResourceManagerException;
import io.atomix.manager.resource.internal.InstanceOperation;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManagerState extends StateMachine implements SessionListener, Snapshottable {
  private StateMachineExecutor executor;
  private final Map<String, Long> keys = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final ResourceManagerCommitPool commits = new ResourceManagerCommitPool();

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
    executor.register(InstanceOperation.class, (Function<Commit<InstanceOperation>, Object>) this::operateResource);
    executor.register(GetResource.class, this::getResource);
    executor.register(GetResourceIfExists.class, this::getResourceIfExists);
    executor.register(CloseResource.class, this::closeResource);
    executor.register(DeleteResource.class, this::deleteResource);
    executor.register(ResourceExists.class, this::resourceExists);
    executor.register(GetResourceKeys.class, this::getResourceKeys);
  }

  @Override
  public void snapshot(SnapshotWriter writer) {
    List<ResourceHolder> resources = new ArrayList<>(this.resources.values());
    Collections.sort(resources, (r1, r2) -> (int) (r1.id - r2.id));
    for (ResourceHolder resource : resources) {
      if (resource.stateMachine instanceof Snapshottable) {
        ((Snapshottable) resource.stateMachine).snapshot(writer);
      }
    }
  }

  @Override
  public void install(SnapshotReader reader) {
    List<ResourceHolder> resources = new ArrayList<>(this.resources.values());
    Collections.sort(resources, (r1, r2) -> (int)(r1.id - r2.id));
    for (ResourceHolder resource : resources) {
      if (resource.stateMachine instanceof Snapshottable) {
        ((Snapshottable) resource.stateMachine).install(reader);
      }
    }
  }

  /**
   * Performs an operation on a resource.
   */
  @SuppressWarnings("unchecked")
  private Object operateResource(Commit<InstanceOperation> commit) {
    long resourceId = commit.operation().resource();
    ResourceHolder resource = resources.get(resourceId);
    if (resource == null) {
      commit.close();
      throw new ResourceManagerException("unknown resource: " + resourceId);
    }

    // If the session exists for the resource, use the existing session.
    ManagedResourceSession resourceSession = resource.executor.context.sessions.session(commit.session().id());

    // If the session is not open for this resource, add the commit session to the resource.
    if (resourceSession == null) {
      resourceSession = new ManagedResourceSession(resourceId, commit, commit.session());
    }

    // Execute the operation.
    return resource.executor.execute(commits.acquire(commit, resourceSession));
  }

  /**
   * Gets a resource.
   */
  protected long getResource(Commit<? extends GetResource> commit) {
    String key = commit.operation().key();
    ResourceType type = commit.operation().type();

    // Lookup the resource ID for the resource key.
    Long resourceId = keys.get(key);

    // If no resource ID was found, create the resource.
    if (resourceId == null) {

      // The first time a resource is created, the resource ID is the index of the commit that created it.
      resourceId = commit.index();
      keys.put(key, resourceId);

      try {
        // For the new resource, construct a state machine and store the resource info.
        ResourceStateMachine stateMachine = type.factory().newInstance().createStateMachine(new Resource.Config(commit.operation().config()));
        ResourceManagerStateMachineExecutor executor = new ResourceManagerStateMachineExecutor(resourceId, this.executor);

        // Store the resource to be referenced by its resource ID.
        ResourceHolder resource = new ResourceHolder(resourceId, key, type, commit, stateMachine, executor);
        resources.put(resourceId, resource);

        // Initialize the resource state machine.
        stateMachine.init(executor);

        // Create a resource session for the client resource instance.
        ManagedResourceSession resourceSession = new ManagedResourceSession(resourceId, commit, commit.session());
        resource.executor.context.sessions.register(resourceSession);

        // Returns the session ID for the resource client session.
        return resourceId;
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      // If a resource was found, validate that the resource type matches.
      ResourceHolder resource = resources.get(resourceId);
      if (resource == null || !resource.type.equals(type)) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }

      // Create a resource session for the client resource instance.
      ManagedResourceSession resourceSession = new ManagedResourceSession(resourceId, commit, commit.session());
      resource.executor.context.sessions.register(resourceSession);

      return resourceId;
    }
  }

  /**
   * Applies a get resource if exists commit.
   */
  @SuppressWarnings("unchecked")
  private long getResourceIfExists(Commit<GetResourceIfExists> commit) {
    String key = commit.operation().key();

    // Lookup the resource ID for the resource key.
    Long resourceId = keys.get(key);
    if (resourceId != null) {
      return getResource(commit);
    }
    return 0;
  }

  /**
   * Checks if a resource exists.
   */
  protected boolean resourceExists(Commit<ResourceExists> commit) {
    try {
      return keys.containsKey(commit.operation().key());
    } finally {
      commit.close();
    }
  }

  /**
   * Closes a resource.
   */
  protected void closeResource(Commit<CloseResource> commit) {
    try {
      long resourceId = commit.operation().resource();
      ResourceHolder resource = resources.get(resourceId);
      if (resource == null) {
        throw new ResourceManagerException("unknown resource: " + resourceId);
      }

      resource.executor.context.sessions.unregister(commit.session().id());
      resource.executor.context.sessions.close(commit.session().id());
    } finally {
      commit.close();
    }
  }

  /**
   * Applies a delete resource commit.
   */
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    try {
      ResourceHolder resource = resources.remove(commit.operation().resource());
      if (resource == null) {
        throw new ResourceManagerException("unknown resource: " + commit.operation().resource());
      }

      // Delete the resource state machine and close the resource state machine executor.
      resource.stateMachine.delete();
      resource.executor.close();

      keys.remove(resource.key);
      return true;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles get resource keys commit.
   */
  protected Set<String> getResourceKeys(Commit<GetResourceKeys> commit) {
    try {
      if (commit.operation().type() == 0) {
        return new HashSet<>(keys.keySet());
      }

      return new HashSet<>(resources.entrySet()
        .stream()
        .filter(e -> e.getValue().type.id() == commit.operation().type())
        .map(e -> e.getValue().key)
        .collect(Collectors.toSet()));
    } finally {
      commit.close();
    }
  }

  @Override
  public void register(ServerSession session) {
  }

  @Override
  public void expire(ServerSession session) {
    for (ResourceHolder resource : resources.values()) {
      resource.executor.context.sessions.expire(session.id());
    }
  }

  @Override
  public void unregister(ServerSession session) {
    for (ResourceHolder resource : resources.values()) {
      resource.executor.context.sessions.unregister(session.id());
    }
  }

  @Override
  public void close(ServerSession session) {
    for (ResourceHolder resource : resources.values()) {
      resource.executor.context.sessions.close(session.id());
    }
  }

  /**
   * Resource holder.
   */
  private static class ResourceHolder {
    private final long id;
    private final String key;
    private final ResourceType type;
    private final Commit<? extends GetResource> commit;
    private final ResourceStateMachine stateMachine;
    private final ResourceManagerStateMachineExecutor executor;

    private ResourceHolder(long id, String key, ResourceType type, Commit<? extends GetResource> commit, ResourceStateMachine stateMachine, ResourceManagerStateMachineExecutor executor) {
      this.id = id;
      this.key = key;
      this.type = type;
      this.commit = commit;
      this.stateMachine = stateMachine;
      this.executor = executor;
    }
  }

  /**
   * Session holder.
   */
  private static class SessionHolder {
    private final Commit commit;
    private final ManagedResourceSession session;

    private SessionHolder(Commit commit, ManagedResourceSession session) {
      this.commit = commit;
      this.session = session;
    }
  }

}
