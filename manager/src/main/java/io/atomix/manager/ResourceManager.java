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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Provides an interface for creating and operating on {@link io.atomix.resource.Resource}s remotely.
 *
 * @param <T> resource type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ResourceManager<T extends ResourceManager<T>> {

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
  ThreadContext context();

  /**
   * Returns the resource manager serializer.
   * <p>
   * The serializer applies to all resources controlled by this instance. Serializable types registered
   * on the serializer will be reflected at the {@link io.atomix.catalyst.transport.Transport} layer.
   * Types registered on the client must also be registered on the server for deserialization.
   *
   * @return The resource manager serializer.
   */
  Serializer serializer();

  /**
   * Returns the resource type for the given resource class.
   *
   * @param type The resource class.
   * @return The resource type for the given resource class.
   * @throws IllegalArgumentException if {@code type} is unregistered
   */
  ResourceType type(Class<? extends Resource<?>> type);

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
   *     DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.exists("lock").thenAccept(exists -> {
   *     if (!exists) {
   *       atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
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
  CompletableFuture<Boolean> exists(String key);

  /**
   * Returns keys of all existing resources.
   *
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   Collection<String> resourceKeys = atomix.keys().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<Collection<String>>keys().thenAccept(resourceKeys -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with the keys of all existing resources.
   */
  CompletableFuture<Set<String>> keys();

  /**
   * Returns the keys of existing resources belonging to a resource type.
   *
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   Set<String> resourceKeys = atomix.keys(DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<Set<String>>keys().thenAccept(resourceKeys -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param type The resource type by which to filter resources.
   * @param <T> The resource type.
   * @return A completable future to be completed with the set of resource keys.
   * @throws NullPointerException if {@code type} is null
   */
  <T extends Resource> CompletableFuture<Set<String>> keys(Class<? super T> type);

  /**
   * Returns the keys of existing resources belonging to a resource type.
   *
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   Set<String> resourceKeys = atomix.keys(DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<Set<String>>keys().thenAccept(resourceKeys -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param type The resource type by which to filter resources.
   * @return A completable future to be completed with the set of resource keys.
   */
  CompletableFuture<Set<String>> keys(ResourceType type);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
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
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param config The cluster-wide resource configuration.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Config config);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param options The local resource options.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Options options);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param config The cluster-wide resource configuration.
   * @param options The local resource options.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Config config, Resource.Options options);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
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
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param config The cluster-wide resource configuration.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Config config);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param options The local resource options.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Options options);

  /**
   * Gets or creates the given resource and acquires a singleton reference to it.
   * <p>
   * If a resource at the given key already exists, the resource will be validated to verify that its type
   * matches the given type. If no resource yet exists, a new resource will be created in the cluster. Once
   * the session for the resource has been opened, a resource instance will be returned.
   * <p>
   * The returned {@link Resource} instance will be a singleton reference to an global instance for this node.
   * That is, multiple calls to this method for the same resource will result in the same {@link Resource}
   * instance being returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method:
   * <pre>
   *   {@code
   *   DistributedLock lock = atomix.getResource("lock", DistributedLock.class).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the result is received in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   atomix.<DistributedLock>getResource("lock", DistributedLock.class).thenAccept(lock -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key at which to get the resource.
   * @param type The expected resource type.
   * @param config The cluster-wide resource configuration.
   * @param options The local resource options.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been loaded.
   * @throws NullPointerException if {@code key} or {@code type} are null
   * @throws IllegalArgumentException if {@code type} is inconsistent with a previously created type
   */
  <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Config config, Resource.Options options);

}
