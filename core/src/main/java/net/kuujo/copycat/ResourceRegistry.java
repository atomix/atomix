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
package net.kuujo.copycat;

import net.kuujo.copycat.io.util.ClassPath;
import net.kuujo.copycat.raft.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles registration of resource types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceRegistry.class);
  private final Map<Class<? extends Resource>, Class<? extends StateMachine>> resources = new HashMap<>();

  public ResourceRegistry() {
    this(Thread.currentThread().getContextClassLoader());
  }

  public ResourceRegistry(ClassLoader classLoader) {
    try {
      registerResources(classLoader);
    } catch (IOException e) {
      throw new ConfigurationException("failed to initialize resource registry", e);
    }
  }

  /**
   * Registers all resources from the classpath.
   */
  @SuppressWarnings("unchecked")
  private void registerResources(ClassLoader classLoader) throws IOException {
    ClassPath classPath = ClassPath.from(classLoader);
    for (ClassPath.ClassInfo info : classPath.getAllClasses()) {
      Class<?> type;
      try {
        type = info.load();
      } catch (LinkageError e) {
        continue;
      }

      if (Resource.class.isAssignableFrom(type)) {
        Stateful stateful = type.getAnnotation(Stateful.class);
        if (stateful != null) {
          register((Class<? extends Resource>) type, stateful.value());
        }
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
  public <T extends Resource> ResourceRegistry register(Class<? extends Resource> resourceType) {
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
    LOGGER.info("Registered resource {} with state machine: {}", resourceType, stateMachineType);
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
  @SuppressWarnings("unchecked")
  public Class<? extends StateMachine> lookup(Class<?> resource) {
    return resources.get(resource);
  }

}
