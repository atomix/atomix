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
package io.atomix.manager.state;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.resource.InstanceOperation;
import io.atomix.resource.ResourceRegistry;
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
  private final ResourceRegistry registry;
  private StateMachineExecutor executor;
  private final Map<String, Long> keys = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final ResourceManagerCommitPool commits = new ResourceManagerCommitPool();

  public ResourceManagerState(ResourceRegistry registry) {
    this.registry = Assert.notNull(registry, "registry");
  }

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

    ServerSession session;

    // If the session exists for the resource, use the existing session.
    SessionHolder sessionHolder = resource.sessions.get(commit.session().id());
    if (sessionHolder != null) {
      session = sessionHolder.session;
    }
    // If the commit session is not open for this resource, add the commit session to the resource.
    else {
      session = new ManagedResourceSession(resourceId, commit.session());
    }

    // Execute the operation.
    return resource.executor.execute(commits.acquire(commit, session));
  }

  /**
   * Gets a resource.
   */
  protected long getResource(Commit<? extends GetResource> commit) {
    String key = commit.operation().key();
    ResourceType type = registry.lookup(commit.operation().type());

    // If the resource type is not known, fail the get.
    if (type == null) {
      commit.close();
      throw new IllegalArgumentException("unknown resource type: " + commit.operation().type());
    }

    // Lookup the resource ID for the resource key.
    Long resourceId = keys.get(key);

    // If no resource ID was found, create the resource.
    if (resourceId == null) {

      // The first time a resource is created, the resource ID is the index of the commit that created it.
      resourceId = commit.index();
      keys.put(key, resourceId);

      try {
        // For the new resource, construct a state machine and store the resource info.
        ResourceStateMachine stateMachine = type.stateMachine().newInstance();
        ResourceManagerStateMachineExecutor executor = new ResourceManagerStateMachineExecutor(resourceId, this.executor);

        // Store the resource to be referenced by its resource ID.
        ResourceHolder resourceHolder = new ResourceHolder(resourceId, key, type, commit, stateMachine, executor);
        resources.put(resourceId, resourceHolder);

        // Initialize the resource state machine.
        stateMachine.init(executor);

        // Create a resource session for the client resource instance.
        ManagedResourceSession session = new ManagedResourceSession(resourceId, commit.session());
        SessionHolder holder = new SessionHolder(commit, session);
        resourceHolder.sessions.put(commit.session().id(), holder);

        // Register the newly created session with the resource state machine.
        stateMachine.register(session);

        // Returns the session ID for the resource client session.
        return resourceId;
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      // If a resource was found, validate that the resource type matches.
      ResourceHolder resourceHolder = resources.get(resourceId);
      if (resourceHolder == null || !resourceHolder.type.equals(type)) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }

      // If the session is not already associated with the resource, attach the session to the resource.
      // Otherwise, if the session already has a multiton instance of the resource open, clean the commit.
      SessionHolder holder = resourceHolder.sessions.get(commit.session().id());
      if (holder == null) {
        // Crete a resource session for the client resource instance.
        ManagedResourceSession session = new ManagedResourceSession(resourceId, commit.session());
        holder = new SessionHolder(commit, session);
        resourceHolder.sessions.put(commit.session().id(), holder);

        // Register the newly created session with the resource state machine.
        resourceHolder.stateMachine.register(session);

        return resourceId;
      } else {
        // Return the resource client session ID and clean the commit since no new resource or session was created.
        commit.close();
        return holder.session.id();
      }
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
    long resourceId = commit.operation().resource();
    ResourceHolder resourceHolder = resources.get(resourceId);
    if (resourceHolder == null) {
      commit.close();
      throw new ResourceManagerException("unknown resource: " + resourceId);
    }

    SessionHolder sessionHolder = resourceHolder.sessions.remove(commit.session().id());
    if (sessionHolder != null) {
      try {
        resourceHolder.stateMachine.unregister(sessionHolder.session);
        resourceHolder.stateMachine.close(sessionHolder.session);
      } finally {
        // Ensure that the commit that created the resource is not closed even of the client closed its reference to it.
        if (sessionHolder.commit != resourceHolder.commit) {
          sessionHolder.commit.close();
        }
      }
    }
  }

  /**
   * Applies a delete resource commit.
   */
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    try {
      ResourceHolder resourceHolder = resources.remove(commit.operation().resource());
      if (resourceHolder == null) {
        throw new ResourceManagerException("unknown resource: " + commit.operation().resource());
      }

      // Delete the resource state machine and close the resource state machine executor.
      try {
        resourceHolder.stateMachine.delete();
        resourceHolder.executor.close();
      } finally {
        resourceHolder.commit.close();
      }

      // Close all commits that opened sessions to the resource.
      // Don't close the commit that created the resource since that was closed above.
      for (SessionHolder sessionHolder : resourceHolder.sessions.values()) {
        if (sessionHolder.commit != resourceHolder.commit) {
          sessionHolder.commit.close();
        }
      }

      keys.remove(resourceHolder.key);
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

      ResourceType resourceType = registry.lookup(commit.operation().type());
      if (resourceType == null) {
        throw new IllegalArgumentException("unknown resource type: " + commit.operation().type());
      }

      return new HashSet<>(resources.entrySet()
        .stream()
        .filter(e -> e.getValue().type.equals(resourceType))
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
      SessionHolder sessionHolder = resource.sessions.get(session.id());
      if (sessionHolder != null) {
        resource.stateMachine.expire(sessionHolder.session);
      }
    }
  }

  @Override
  public void unregister(ServerSession session) {
    for (ResourceHolder resource : resources.values()) {
      SessionHolder sessionHolder = resource.sessions.get(session.id());
      if (sessionHolder != null) {
        resource.stateMachine.unregister(sessionHolder.session);
      }
    }
  }

  @Override
  public void close(ServerSession session) {
    for (ResourceHolder resource : resources.values()) {
      SessionHolder sessionHolder = resource.sessions.remove(session.id());
      if (sessionHolder != null) {
        resource.stateMachine.close(sessionHolder.session);

        // Ensure that the commit that created the resource is not closed when the session that created it is closed.
        if (resource.commit != sessionHolder.commit) {
          sessionHolder.commit.close();
        }
      }
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
    private final Map<Long, SessionHolder> sessions = new HashMap<>();

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
