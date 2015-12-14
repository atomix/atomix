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
package io.atomix.manager;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
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
public class ResourceManager extends StateMachine implements SessionListener, Snapshottable {
  private final ResourceRegistry registry;
  private StateMachineExecutor executor;
  private final Map<String, Long> keys = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final Map<Long, SessionHolder> sessions = new HashMap<>();
  private final ResourceManagerCommitPool commits = new ResourceManagerCommitPool();

  public ResourceManager(ResourceRegistry registry) {
    this.registry = Assert.notNull(registry, "registry");
  }

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
    executor.register(InstanceOperation.class, (Function<Commit<InstanceOperation>, Object>) this::operateResource);
    executor.register(GetResource.class, this::getResource);
    executor.register(GetResourceIfExists.class, this::getResourceIfExists);
    executor.register(CreateResource.class, this::createResource);
    executor.register(CreateResourceIfExists.class, this::createResourceIfExists);
    executor.register(DeleteResource.class, this::deleteResource);
    executor.register(ResourceExists.class, this::resourceExists);
    executor.register(GetResourceKeys.class, this::getResourceKeys);
  }

  @Override
  public void snapshot(SnapshotWriter writer) {
    List<ResourceHolder> resources = new ArrayList<>(this.resources.values());
    Collections.sort(resources, (r1, r2) -> (int)(r1.id - r2.id));
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
    ResourceHolder resource;

    // Get the session and resource associated with the commit.
    SessionHolder session = sessions.get(commit.operation().resource());
    if (session != null) {
      resource = resources.get(session.resource);
    } else {
      try {
        throw new ResourceManagerException("unknown resource session: " + commit.operation().resource());
      } finally {
        commit.close();
      }
    }

    return resource.executor.execute(commits.acquire(commit, session.session));
  }

  /**
   * Gets a resource.
   */
  protected long getResource(Commit<? extends GetResource> commit) {
    String key = commit.operation().key();
    ResourceType<?> type = registry.lookup(commit.operation().type());

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
        ResourceHolder resource = new ResourceHolder(resourceId, key, type, stateMachine, executor);
        resources.put(resourceId, resource);

        // Initialize the resource state machine.
        stateMachine.init(executor);

        // Create a resource session for the client resource instance.
        ManagedResourceSession session = new ManagedResourceSession(commit.index(), commit.session());
        SessionHolder holder = new SessionHolder(resourceId, commit, session);
        sessions.put(session.id(), holder);
        resource.sessions.put(commit.session().id(), holder);

        // Register the newly created session with the resource state machine.
        if (stateMachine instanceof SessionListener) {
          ((SessionListener) stateMachine).register(session);
        }

        // Returns the session ID for the resource client session.
        return session.id();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      // If a resource was found, validate that the resource type matches.
      ResourceHolder resource = resources.get(resourceId);
      if (resource == null || !resource.type.equals(type)) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }

      // If the session is not already associated with the resource, attach the session to the resource.
      // Otherwise, if the session already has a multiton instance of the resource open, clean the commit.
      SessionHolder holder = resource.sessions.get(commit.session().id());
      if (holder == null) {
        // Crete a resource session for the client resource instance.
        ManagedResourceSession session = new ManagedResourceSession(commit.index(), commit.session());
        holder = new SessionHolder(resourceId, commit, session);
        sessions.put(session.id(), holder);
        resource.sessions.put(commit.session().id(), holder);

        // Register the newly created session with the resource state machine.
        if (resource.stateMachine instanceof SessionListener) {
          ((SessionListener) resource.stateMachine).register(session);
        }

        return session.id();
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
   * Applies a create resource commit.
   */
  private long createResource(Commit<? extends CreateResource> commit) {
    String key = commit.operation().key();
    ResourceType<?> type = registry.lookup(commit.operation().type());

    // If the resource type is not known, fail the get.
    if (type == null) {
      commit.close();
      throw new IllegalArgumentException("unknown resource type: " + commit.operation().type());
    }

    // Get the resource ID for the key.
    Long resourceId = keys.get(key);

    ResourceHolder resource;

    // If no resource yet exists, create a new resource state machine with the commit index as the resource ID.
    if (resourceId == null) {

      // The first time a resource is created, the resource ID is the index of the commit that created it.
      resourceId = commit.index();
      keys.put(key, resourceId);

      try {
        // For the new resource, construct a state machine and store the resource info.
        ResourceStateMachine stateMachine = type.stateMachine().newInstance();
        ResourceManagerStateMachineExecutor executor = new ResourceManagerStateMachineExecutor(resourceId, this.executor);

        // Store the resource to be referenced by its resource ID.
        resource = new ResourceHolder(resourceId, key, type, stateMachine, executor);
        resources.put(resourceId, resource);

        // Initialize the resource state machine.
        stateMachine.init(executor);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      // If a resource was found, validate that the resource type matches.
      resource = resources.get(resourceId);
      if (resource == null || !resource.type.equals(type)) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }
    }

    // The resource ID for the unique resource session is the commit index.
    long id = commit.index();

    // Create the resource session and register the session with the resource state machine.
    ManagedResourceSession session = new ManagedResourceSession(id, commit.session());
    sessions.put(id, new SessionHolder(resourceId, commit, session));

    // Register the newly created session with the resource state machine.
    if (resource.stateMachine instanceof SessionListener) {
      ((SessionListener) resource.stateMachine).register(session);
    }

    return id;
  }

  /**
   * Applies a create resource if exists commit.
   */
  @SuppressWarnings("unchecked")
  private long createResourceIfExists(Commit<CreateResourceIfExists> commit) {
    String key = commit.operation().key();

    // Lookup the resource ID for the resource key.
    Long resourceId = keys.get(key);
    if (resourceId != null) {
      return createResource(commit);
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
   * Applies a delete resource commit.
   */
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    try {
      ResourceHolder resource = resources.remove(commit.operation().resource());
      if (resource == null) {
        throw new ResourceManagerException("unknown resource: " + commit.operation().resource());
      }

      resource.stateMachine.delete();
      resource.executor.close();
      keys.remove(resource.key);

      Iterator<Map.Entry<Long, SessionHolder>> iterator = sessions.entrySet().iterator();
      while (iterator.hasNext()) {
        SessionHolder session = iterator.next().getValue();
        if (session.resource == commit.operation().resource()) {
          iterator.remove();
          session.commit.close();
        }
      }
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
        return keys.keySet();
      }

      ResourceType resourceType = registry.lookup(commit.operation().type());
      if (resourceType == null) {
        throw new IllegalArgumentException("unknown resource type: " + commit.operation().type());
      }

      return resources.entrySet()
        .stream()
        .filter(e -> e.getValue().type.equals(resourceType))
        .map(e -> e.getValue().key)
        .collect(Collectors.toSet());
    } finally {
      commit.close();
    }
  }

  @Override
  public void register(Session session) {

  }

  @Override
  public void expire(Session session) {
    for (SessionHolder sessionHolder : sessions.values()) {
      if (sessionHolder.commit.session().id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          if (resource.stateMachine instanceof SessionListener) {
            ((SessionListener) resource.stateMachine).expire(sessionHolder.session);
          }
        }
      }
    }
  }

  @Override
  public void unregister(Session session) {
    for (SessionHolder sessionHolder : sessions.values()) {
      if (sessionHolder.commit.session().id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          if (resource.stateMachine instanceof SessionListener) {
            ((SessionListener) resource.stateMachine).unregister(sessionHolder.session);
          }
        }
      }
    }
  }

  @Override
  public void close(Session session) {
    Iterator<Map.Entry<Long, SessionHolder>> iterator = sessions.entrySet().iterator();
    while (iterator.hasNext()) {
      SessionHolder sessionHolder = iterator.next().getValue();
      if (sessionHolder.commit.session().id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          resource.sessions.remove(sessionHolder.commit.session().id());
          if (resource.stateMachine instanceof SessionListener) {
            ((SessionListener) resource.stateMachine).close(sessionHolder.session);
          }
        }
        sessionHolder.commit.close();
        iterator.remove();
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
    private final ResourceStateMachine stateMachine;
    private final ResourceManagerStateMachineExecutor executor;
    private final Map<Long, SessionHolder> sessions = new HashMap<>();

    private ResourceHolder(long id, String key, ResourceType type, ResourceStateMachine stateMachine, ResourceManagerStateMachineExecutor executor) {
      this.id = id;
      this.key = key;
      this.type = type;
      this.stateMachine = stateMachine;
      this.executor = executor;
    }
  }

  /**
   * Session holder.
   */
  private static class SessionHolder {
    private final long resource;
    private final Commit commit;
    private final ManagedResourceSession session;

    private SessionHolder(long resource, Commit commit, ManagedResourceSession session) {
      this.resource = resource;
      this.commit = commit;
      this.session = session;
    }
  }

}
