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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.copycat.client.CopycatClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Resource instance manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class InstanceManager {
  private final CopycatClient client;
  private final Map<String, Resource<?>> instances = new HashMap<>();
  private final Map<String, CompletableFuture> futures = new HashMap<>();
  private final Set<Resource<?>> resources = new HashSet<>();

  public InstanceManager(CopycatClient client) {
    this.client = Assert.notNull(client, "client");
  }

  /**
   * Gets a resource instance.
   *
   * @param key The resource key.
   * @param type The resource type info.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been opened.
   */
  @SuppressWarnings("unchecked")
  public synchronized <T extends Resource> CompletableFuture<T> get(String key, ResourceType<T> type) {
    T resource;

    // Determine whether a singleton instance of the given resource key already exists.
    Resource<?> check = instances.get(key);
    if (check == null) {
      Instance<T> instance = new Instance<>(key, type, Instance.Method.GET, this);
      InstanceClient client = new InstanceClient(instance, this.client);
      check = type.factory().create(client);
      instances.put(key, check);
      resources.add(check);
    }

    // Ensure the existing singleton instance type matches the requested instance type. If the instance
    // was created new, this condition will always pass. If there was another instance created of a
    // different type, an exception will be returned without having to make a request to the cluster.
    if (!check.type().equals(type)) {
      return Futures.exceptionalFuture(new IllegalArgumentException("inconsistent resource type: " + type));
    }

    resource = (T) check;

    // Ensure if a singleton instance is already being created, the existing open future is returned.
    CompletableFuture<T> future = futures.get(key);
    if (future == null) {
      future = resource.open();
      futures.put(key, future);
    }
    return future;
  }

  /**
   * Creates a resource instance.
   *
   * @param key The resource key.
   * @param type The resource type info.
   * @param <T> The resource type.
   * @return A completable future to be completed once the resource has been created.
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> create(String key, ResourceType<T> type) {
    Instance<T> instance = new Instance<>(key, type, Instance.Method.GET, this);
    InstanceClient client = new InstanceClient(instance, this.client);
    T resource = type.factory().create(client);
    synchronized (this) {
      resources.add(resource);
    }
    return resource.open();
  }

  /**
   * Closes the given resource instance.
   *
   * @param instance The instance to close.
   */
  void close(Instance<?> instance) {
    if (instance.method() == Instance.Method.GET) {
      synchronized (this) {
        instances.remove(instance.key());
        futures.remove(instance.key());
      }
    }
  }

  /**
   * Closes all instances.
   *
   * @return A completable future to be completed once all instances have been closed.
   */
  public synchronized CompletableFuture<Void> close() {
    CompletableFuture<?>[] futures = new CompletableFuture[resources.size()];
    int i = 0;
    for (Resource<?> instance : resources) {
      futures[i++] = instance.close();
    }
    return CompletableFuture.allOf(futures);
  }

}
