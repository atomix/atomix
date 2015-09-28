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

import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.resource.ResourceOperation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager extends StateMachine {
  private StateMachineExecutor executor;
  private final Map<String, Long> paths = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final Map<Long, SessionHolder> sessions = new HashMap<>();
  private final ResourceCommitPool commits = new ResourceCommitPool();

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
    executor.register(ResourceOperation.class, (Function<Commit<ResourceOperation>, Object>) this::operateResource);
    executor.register(GetResource.class, this::getResource);
    executor.register(CreateResource.class, this::createResource);
    executor.register(DeleteResource.class, this::deleteResource);
    executor.register(ResourceExists.class, this::resourceExists);
  }

  /**
   * Performs an operation on a resource.
   */
  @SuppressWarnings("unchecked")
  private Object operateResource(Commit<ResourceOperation> commit) {
    ResourceHolder resource;
    SessionHolder session = sessions.get(commit.operation().resource());
    if (session != null) {
      resource = resources.get(session.resource);
    } else {
      resource = resources.get(commit.operation().resource());
    }

    if (resource == null) {
      throw new ResourceManagerException("unknown resource: " + commit.operation().resource());
    }

    if (session == null) {
      session = resource.sessions.get(commit.session().id());
      if (session == null) {
        throw new ResourceManagerException("unknown resource session: " + commit.session().id());
      }
    }

    ResourceCommit resourceCommit = commits.acquire(commit, session.session);
    return resource.executor.execute(resourceCommit);
  }

  /**
   * Gets a resource.
   */
  protected long getResource(Commit<GetResource> commit) {
    String path = commit.operation().path();

    Long resourceId = paths.get(path);

    if (resourceId == null) {
      resourceId = commit.index();
      paths.put(path, commit.index());

      try {
        StateMachine stateMachine = commit.operation().type().newInstance();
        ResourceStateMachineExecutor executor = new ResourceStateMachineExecutor(commit.index(), this.executor);
        ResourceHolder resource = new ResourceHolder(path, stateMachine, executor);
        resources.put(resourceId, resource);
        stateMachine.init(executor);
        resource.sessions.put(commit.session().id(), new SessionHolder(resourceId, commit, new ManagedResourceSession(resourceId, commit.session())));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      ResourceHolder resource = resources.get(resourceId);
      if (resource == null || resource.stateMachine.getClass() != commit.operation().type()) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }
      commit.clean();
    }
    return resourceId;
  }

  /**
   * Applies a create resource commit.
   */
  private long createResource(Commit<CreateResource> commit) {
    String path = commit.operation().path();

    Long resourceId = paths.get(path);

    long id;
    ResourceHolder resource;
    if (resourceId == null) {
      resourceId = commit.index();
      paths.put(path, commit.index());

      try {
        StateMachine stateMachine = commit.operation().type().newInstance();
        ResourceStateMachineExecutor executor = new ResourceStateMachineExecutor(commit.index(), this.executor);
        resource = new ResourceHolder(path, stateMachine, executor);
        resources.put(resourceId, resource);
        stateMachine.init(executor);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      resource = resources.get(resourceId);
      if (resource == null || resource.stateMachine.getClass() != commit.operation().type()) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }
    }

    id = commit.index();
    ManagedResourceSession session = new ManagedResourceSession(id, commit.session());
    sessions.put(id, new SessionHolder(resourceId, commit, session));
    resource.stateMachine.register(session);
    return id;
  }

  /**
   * Checks if a resource exists.
   */
  protected boolean resourceExists(Commit<ResourceExists> commit) {
    return paths.containsKey(commit.operation().path());
  }

  /**
   * Applies a delete resource commit.
   */
  protected boolean deleteResource(Commit<DeleteResource> commit) {
    ResourceHolder resource = resources.remove(commit.operation().resource());
    if (resource == null) {
      throw new ResourceManagerException("unknown resource: " + commit.operation().resource());
    }

    resource.executor.close();
    paths.remove(resource.path);

    Iterator<Map.Entry<Long, SessionHolder>> iterator = sessions.entrySet().iterator();
    while (iterator.hasNext()) {
      SessionHolder session = iterator.next().getValue();
      if (session.resource == commit.operation().resource()) {
        iterator.remove();
        session.commit.clean();
      }
    }
    return true;
  }

  @Override
  public void expire(Session session) {
    for (ResourceHolder resource : resources.values()) {
      SessionHolder resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.stateMachine.expire(resourceSession.session);
      }
    }

    for (SessionHolder sessionHolder : sessions.values()) {
      if (sessionHolder.session.id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          resource.stateMachine.expire(sessionHolder.session);
        }
      }
    }
  }

  @Override
  public void close(Session session) {
    for (ResourceHolder resource : resources.values()) {
      SessionHolder resourceSession = resource.sessions.remove(session.id());
      if (resourceSession != null) {
        resource.stateMachine.close(resourceSession.session);
        resourceSession.commit.clean();
      }
    }

    Iterator<Map.Entry<Long, SessionHolder>> iterator = sessions.entrySet().iterator();
    while (iterator.hasNext()) {
      SessionHolder sessionHolder = iterator.next().getValue();
      if (sessionHolder.session.id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          resource.stateMachine.close(sessionHolder.session);
        }
        sessionHolder.commit.clean();
        iterator.remove();
      }
    }
  }

  /**
   * Resource holder.
   */
  private static class ResourceHolder {
    private final String path;
    private final StateMachine stateMachine;
    private final ResourceStateMachineExecutor executor;
    private final Map<Long, SessionHolder> sessions = new HashMap<>();

    private ResourceHolder(String path, StateMachine stateMachine, ResourceStateMachineExecutor executor) {
      this.path = path;
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
