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
package io.atomix.copycat.manager;

import io.atomix.catalog.client.session.Session;
import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadPoolContext;
import io.atomix.copycat.resource.ResourceOperation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceManager extends StateMachine {
  private final ScheduledExecutorService scheduler;
  private StateMachineExecutor executor;
  private final Map<String, Long> paths = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
  private final Map<Long, SessionHolder> sessions = new HashMap<>();
  private final ResourceCommitPool commits = new ResourceCommitPool();

  /**
   * @throws NullPointerException if {@code scheduler} is null
   */
  public ResourceManager(ScheduledExecutorService scheduler) {
    this.scheduler = Assert.notNull(scheduler, "scheduler");
  }

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
    executor.register(ResourceOperation.class, (Function<Commit<ResourceOperation>, CompletableFuture<Object>>) this::operateResource);
    executor.register(GetResource.class, this::getResource);
    executor.register(CreateResource.class, this::createResource);
    executor.register(DeleteResource.class, this::deleteResource);
    executor.register(ResourceExists.class, this::resourceExists);
  }

  /**
   * Performs an operation on a resource.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> operateResource(Commit<ResourceOperation> commit) {
    ResourceHolder resource;
    ManagedResourceSession session = null;

    SessionHolder client = sessions.get(commit.operation().resource());
    if (client != null) {
      resource = resources.get(client.resource);
      session = client.session;
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

    CompletableFuture<Object> future = new CompletableFuture<>();

    ResourceCommit resourceCommit = commits.acquire(commit, session);
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
        ThreadContext context = new ThreadPoolContext(scheduler, ThreadContext.currentContext().serializer().clone());
        ResourceStateMachineExecutor executor = new ResourceStateMachineExecutor(commit.index(), this.executor, context);
        ResourceHolder resource = new ResourceHolder(path, stateMachine, executor);
        resources.put(resourceId, resource);
        stateMachine.init(executor);
        resource.sessions.put(commit.session().id(), new ManagedResourceSession(resourceId, commit.session()));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceManagerException("failed to instantiate state machine", e);
      }
    } else {
      ResourceHolder resource = resources.get(resourceId);
      if (resource == null || resource.stateMachine.getClass() != commit.operation().type()) {
        throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
      }
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
        ThreadContext context = new ThreadPoolContext(scheduler, ThreadContext.currentContext().serializer().clone());
        ResourceStateMachineExecutor executor = new ResourceStateMachineExecutor(commit.index(), this.executor, context);
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
    sessions.put(id, new SessionHolder(resourceId, session));
    resource.executor.execute(() -> resource.stateMachine.register(session));
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
      if (iterator.next().getValue().resource == commit.operation().resource()) {
        iterator.remove();
      }
    }
    return true;
  }

  @Override
  public void expire(Session session) {
    for (ResourceHolder resource : resources.values()) {
      ManagedResourceSession resourceSession = resource.sessions.get(session.id());
      if (resourceSession != null) {
        resource.executor.execute(() -> resource.stateMachine.expire(resourceSession));
      }
    }

    for (SessionHolder sessionHolder : sessions.values()) {
      if (sessionHolder.session.id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          resource.executor.execute(() -> resource.stateMachine.expire(sessionHolder.session));
        }
      }
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

    Iterator<Map.Entry<Long, SessionHolder>> iterator = sessions.entrySet().iterator();
    while (iterator.hasNext()) {
      SessionHolder sessionHolder = iterator.next().getValue();
      if (sessionHolder.session.id() == session.id()) {
        ResourceHolder resource = resources.get(sessionHolder.resource);
        if (resource != null) {
          resource.executor.execute(() -> resource.stateMachine.close(sessionHolder.session));
        }
        iterator.remove();
      }
    }
  }

  @Override
  public void close() {
    scheduler.shutdown();
  }

  /**
   * Resource holder.
   */
  private static class ResourceHolder {
    private final String path;
    private final StateMachine stateMachine;
    private final ResourceStateMachineExecutor executor;
    private final Map<Long, ManagedResourceSession> sessions = new HashMap<>();

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
    private final ManagedResourceSession session;

    private SessionHolder(long resource, ManagedResourceSession session) {
      this.resource = resource;
      this.session = session;
    }
  }

}
