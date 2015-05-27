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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.raft.StateMachine;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles registration of resource types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceRegistry {
  private static final String COPYCAT_RESOURCES = "net.kuujo.copycat";
  private final Map<Class<? extends Resource>, Class<? extends StateMachine>> resources = new HashMap<>();

  public ResourceRegistry(Object... resources) {
    Object[] allResources = new Object[resources.length + 1];
    System.arraycopy(resources, 0, allResources, 0, resources.length);
    allResources[allResources.length] = COPYCAT_RESOURCES;
    registerResources(resources);
  }

  /**
   * Registers all resources from the classpath.
   */
  @SuppressWarnings("unchecked")
  private void registerResources(Object... resources) {
    Reflections reflections = new Reflections(resources);

    for (Class<? extends Resource> resource : reflections.getSubTypesOf(Resource.class)) {
      Stateful stateful = resource.getAnnotation(Stateful.class);
      if (stateful != null) {
        register(resource, stateful.value());
      }
    }
  }

  /**
   * Registers a resource type.
   *
   * @param resourceType The resource type.
   * @return The resource registry.
   */
  @SuppressWarnings("unchecked")
  public <T extends Resource> ResourceRegistry register(Class<T> resourceType) {
    Stateful stateful = resourceType.getAnnotation(Stateful.class);
    if (stateful != null) {
      register(resourceType, stateful.value());
    } else {
      throw new IllegalArgumentException("unknown resource state: " + resourceType);
    }
    return this;
  }

  /**
   * Registers a resource and state machine.
   *
   * @param resourceType The resource class.
   * @param stateMachineType The state class.
   * @return The resource registry.
   */
  public ResourceRegistry register(Class<? extends Resource> resourceType, Class<? extends StateMachine> stateMachineType) {
    resources.put(resourceType, stateMachineType);
    return this;
  }

  /**
   * Returns the state machine for the given class name.
   *
   * @param name The class name.
   * @return The resource state machine class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends StateMachine> lookup(String name) {
    try {
      return lookup((Class<? extends Resource>) Class.forName(name));
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("unknown resource class: " + name, e);
    }
  }

  /**
   * Returns the state machine for the given class.
   *
   * @param resource The resource class.
   * @return The resource state machine class.
   */
  public Class<? extends StateMachine> lookup(Class<? extends Resource> resource) {
    return resources.get(resource);
  }

}
