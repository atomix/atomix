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
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.Context;
import io.atomix.catalyst.util.concurrent.ThreadPoolContext;
import io.atomix.copycat.resource.ResourceOperation;

import java.util.HashMap;
import java.util.Map;
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
  private final ScheduledExecutorService scheduler;
  private StateMachineExecutor executor;
  private final Map<String, Long> paths = new HashMap<>();
  private final Map<Long, ResourceHolder> resources = new HashMap<>();
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
   * Gets a resource.
   */
  protected long getResource(Commit<GetResource> commit) {
    String path = commit.operation().path();

    Long id = paths.get(path);
    if (id == null) {
      throw new ResourceManagerException("unknown path: " + path);
    }

    ResourceHolder resource = resources.get(id);
    if (resource == null) {
      throw new ResourceManagerException("unknown resource: " + path);
    }

    if (resource.stateMachine.getClass() != commit.operation().type()) {
      throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
    }

    return id;
  }

  /**
   * Applies a create resource commit.
   */
  protected long createResource(Commit<CreateResource> commit) {
    String path = commit.operation().path();

    // Check for an existing instance of a resource at this path.
    Long id = paths.get(path);

    // If a resource already exists, verify that it is of the same type.
    if (id != null) {
      ResourceHolder resource = resources.get(id);
      if (resource != null) {
        if (resource.stateMachine.getClass() != commit.operation().type()) {
          throw new ResourceManagerException("inconsistent resource type: " + commit.operation().type());
        }
        return id;
      }
    }

    id = commit.index();

    try {
      StateMachine resource = commit.operation().type().newInstance();
      Context context = new ThreadPoolContext(scheduler, Context.currentContext().serializer().clone());
      ResourceStateMachineExecutor executor = new ResourceStateMachineExecutor(id, this.executor, context);

      paths.put(path, id);
      resources.put(id, new ResourceHolder(path, resource, executor));

      resource.init(executor.context());
      resource.configure(executor);
      return id;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ResourceManagerException("failed to instantiate state machine", e);
    }
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
    return true;
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
   * Resource holder.
   */
  private static class ResourceHolder {
    private final String path;
    private final Map<Long, ManagedResourceSession> sessions = new HashMap<>();
    private final StateMachine stateMachine;
    private final ResourceStateMachineExecutor executor;

    private ResourceHolder(String path, StateMachine stateMachine, ResourceStateMachineExecutor executor) {
      this.path = path;
      this.stateMachine = stateMachine;
      this.executor = executor;
    }
  }

}
