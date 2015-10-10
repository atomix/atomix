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
package io.atomix;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.manager.CreateResource;
import io.atomix.manager.GetResource;
import io.atomix.manager.ResourceExists;
import io.atomix.resource.ResourceContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Base type for creating and managing distributed {@link DistributedResource resources} in a Atomix cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link DistributedResource} objects via a filesystem like interface. There is a
 * one-to-one relationship between keys and resources, so each key can be associated with one and only one resource.
 * <p>
 * To create a resource, pass the resource {@link java.lang.Class} to the {@link Atomix#create(String, Class)} method.
 * When a resource is created, the {@link io.atomix.copycat.server.StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 * <p>
 * {@link DistributedResource} implementations serve as a user-friendly interface through which to submit
 * {@link io.atomix.copycat.client.Command commands} and {@link io.atomix.copycat.client.Query queries} to the underlying
 * {@link CopycatClient} client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Atomix implements Managed<Atomix> {
  protected final CopycatClient client;
  protected final Transport transport;
  private final Map<Long, ResourceContext> resources = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if {@code client} is null
   */
  protected Atomix(CopycatClient client, Transport transport) {
    this.client = Assert.notNull(client, "client");
    this.transport = Assert.notNull(transport, "transport");
  }

  /**
   * Returns the Atomix thread context.
   * <p>
   * This context is representative of the thread on which asynchronous callbacks will be executed for this
   * Atomix instance. Atomix guarantees that all {@link CompletableFuture}s supplied by this instance will
   * be executed via the returned context. Users can use the context to access the thread-local
   * {@link Serializer}.
   *
   * @return The Atomix thread context.
   */
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Checks whether a resource exists with the given key.
   * <p>
   * If no resource with the given {@code key} exists in the cluster, the returned {@link CompletableFuture} will
   * be completed {@code false}. Note, however, that users should not significantly rely upon the existence or
   * non-existence of a resource due to race conditions. While a resource may not exist when the returned future is
   * completed, it may be created by another node shortly thereafter.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   if (!atomix.exists("lock").get()) {
   *     DistributedLock lock = atomix.create("lock", DistributedLock::new).get();
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.exists("lock").thenAccept(exists -> {
   *     if (!exists) {
   *       atomix.<DistributedLock>create("lock", DistributedLock::new).thenAccept(lock -> {
   *         ...
   *       });
   *     }
   *   });
   *   }
   * </pre>
   *
   * @param key The key to check.
   * @return A completable future indicating whether the given key exists.
   * @throws NullPointerException if {@code key} is null
   */
  public CompletableFuture<Boolean> exists(String key) {
    return client.submit(new ResourceExists(key));
  }

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link DistributedResource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link DistributedResource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.get("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>get("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   */
  @SuppressWarnings("unchecked")
  public <T extends DistributedResource<?>> CompletableFuture<T> get(String key, Class<? super T> type) {
    return get(key, () -> {
      try {
        return (T) type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link DistributedResource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link DistributedResource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.get("lock", DistributedLock::new).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>get("lock", DistributedLock::new).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param factory The resource factory.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code factory} are null
   */
  public <T extends DistributedResource<?>> CompletableFuture<T> get(String key, Supplier<T> factory) {
    T resource = Assert.notNull(factory, "factory").get();
    return client.submit(GetResource.builder()
      .withKey(Assert.notNull(key, "key"))
      .withType(resource.stateMachine())
      .build())
      .thenApply(id -> {
        resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, key, client, transport)));
        return resource;
      });
  }

  /**
   * Creates a new instance for the given resource.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a new resource instance will be returned.
   * <p>
   * The returned {@link DistributedResource} instance will have a unique logical connection to the resource state. This
   * means that operations and events submitted or received by this instance related to this instance only,
   * even if multiple instances of the resource are open on this node. For instance, a lock resource created
   * via this method will behave as a unique reference to the distributed state. Locking a lock acquired via this
   * method will lock <em>only</em> that lock instance and not other instance of the lock on this node.
   * <p>
   * To acquire a singleton reference to a resource that is global to this node, use the {@link #get(String, Class)}
   * method.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.create("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>create("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to create the resource.
   * @param type The resource type to create.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws ResourceException if the resource could not be instantiated
   */
  @SuppressWarnings("unchecked")
  public <T extends DistributedResource<?>> CompletableFuture<T> create(String key, Class<? super T> type) {
    return create(key, () -> {
      try {
        return (T) type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceException(e);
      }
    });
  }

  /**
   * Creates a new instance for the given resource.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a new resource instance will be returned.
   * <p>
   * The returned {@link DistributedResource} instance will have a unique logical connection to the resource state. This
   * means that operations and events submitted or received by this instance related to this instance only,
   * even if multiple instances of the resource are open on this node. For instance, a lock resource created
   * via this method will behave as a unique reference to the distributed state. Locking a lock acquired via this
   * method will lock <em>only</em> that lock instance and not other instance of the lock on this node.
   * <p>
   * To acquire a singleton reference to a resource that is global to this node, use the {@link #get(String, Class)}
   * method.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.create("lock", DistributedLock::new).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>create("lock", DistributedLock::new).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to create the resource.
   * @param factory The resource factory.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   * @throws NullPointerException if {@code key} or {@code factory} are null
   */
  public <T extends DistributedResource<?>> CompletableFuture<T> create(String key, Supplier<T> factory) {
    T resource = Assert.notNull(factory, "factory").get();
    return client.submit(CreateResource.builder()
      .withKey(Assert.notNull(key, "key"))
      .withType(resource.stateMachine())
      .build())
      .thenApply(id -> {
        resource.open(resources.computeIfAbsent(id, i -> new ResourceContext(id, key, client, transport)));
        return resource;
      });
  }

  /**
   * Opens the instance.
   *
   * @return A completable future to be completed once the instance is open.
   */
  @Override
  public CompletableFuture<Atomix> open() {
    return client.open().thenApply(v -> this);
  }

  /**
   * Returns a boolean value indicating whether the instance is open.
   *
   * @return Indicates whether the instance is open.
   */
  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  /**
   * Closes the instance.
   *
   * @return A completable future to be completed once the instance is closed.
   */
  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  /**
   * Returns a boolean value indicating whether the instance is closed.
   *
   * @return Indicates whether the instance is closed.
   */
  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[session=%s]", getClass().getSimpleName(), client.session());
  }

  /**
   * Atomix builder.
   */
  public static abstract class Builder extends io.atomix.catalyst.util.Builder<Atomix> {
    protected CopycatClient.Builder clientBuilder;

    protected Builder(Collection<Address> members) {
      clientBuilder = CopycatClient.builder(members);
    }

    /**
     * Sets the Atomix transport.
     *
     * @param transport The Atomix transport.
     * @return The Atomix builder.
     */
    public Builder withTransport(Transport transport) {
      clientBuilder.withTransport(transport);
      return this;
    }

    /**
     * Sets the Atomix serializer.
     *
     * @param serializer The Atomix serializer.
     * @return The Atomix builder.
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      return this;
    }
  }

}
