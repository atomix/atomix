/*
 * Copyright 2016 the original author or authors.
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resource registry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ResourceRegistry {
  private final Map<Integer, ResourceType> resourceTypes = new ConcurrentHashMap<>();

  public ResourceRegistry() {
  }

  public ResourceRegistry(Collection<ResourceType> resources) {
    resources.forEach(this::register);
  }

  /**
   * Returns a collection of resource types.
   *
   * @return A collection of registered resource types.
   */
  public Collection<ResourceType> types() {
    return resourceTypes.values();
  }

  /**
   * Registers a resource type.
   *
   * @param type The resource type to register.
   * @return The resource registry.
   */
  public ResourceRegistry register(ResourceType type) {
    Assert.notNull(type, "type");
    Assert.argNot(type.id() > Short.MAX_VALUE, "resource ID must be less than %d", Short.MAX_VALUE);
    Assert.argNot(resourceTypes.containsKey(type.id()), "a type with the ID %s already exists", type.id());
    resourceTypes.put(type.id(), type);
    return this;
  }

  /**
   * Looks up a resource type by ID.
   *
   * @param id The resource type ID.
   * @return The resource type.
   */
  public ResourceType lookup(int id) {
    return resourceTypes.get(id);
  }

  @Override
  public String toString() {
    return String.format("%s%s", getClass().getSimpleName(), resourceTypes.values());
  }

}
