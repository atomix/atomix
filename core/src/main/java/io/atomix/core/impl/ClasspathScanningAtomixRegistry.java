/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.impl;

import io.atomix.core.AtomixRegistry;
import io.atomix.utils.NamedType;
import io.atomix.utils.ServiceException;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Atomix registry that scans the classpath for registered objects.
 */
public class ClasspathScanningAtomixRegistry implements AtomixRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathScanningAtomixRegistry.class);

  private final Map<Class<? extends NamedType>, Map<String, NamedType>> registrations = new ConcurrentHashMap<>();

  public ClasspathScanningAtomixRegistry(ClassLoader classLoader, Class<? extends NamedType>... types) {
    this(classLoader, Arrays.asList(types));
  }

  public ClasspathScanningAtomixRegistry(ClassLoader classLoader, Collection<Class<? extends NamedType>> types) {
    final String scanSpec = System.getProperty("io.atomix.scanSpec");
    final ClassGraph classGraph = scanSpec != null ?
            new ClassGraph().enableClassInfo().whitelistPackages(scanSpec).addClassLoader(classLoader) :
            new ClassGraph().enableClassInfo().addClassLoader(classLoader);

    final ScanResult scanResult = classGraph.scan();
    for (Class<? extends NamedType> type : types) {
      Map<String, NamedType> registrations = new ConcurrentHashMap<>();
      scanResult.getClassesImplementing(type.getName()).forEach(classInfo -> {
        if (classInfo.isInterface() || classInfo.isAbstract() || Modifier.isPrivate(classInfo.getModifiers())) {
          return;
        }
        NamedType instance = newInstance(classInfo.loadClass());
        NamedType oldInstance = registrations.put(instance.name(), instance);
        if (oldInstance != null) {
          LOGGER.warn("Found multiple types with name={}, classes=[{}, {}]", instance.name(),
                  oldInstance.getClass().getName(), instance.getClass().getName());
        }
      });
      this.registrations.put(type, registrations);
    }
  }

  /**
   * Instantiates the given type using a no-argument constructor.
   *
   * @param type the type to instantiate
   * @param <T>  the generic type
   * @return the instantiated object
   * @throws ServiceException if the type cannot be instantiated
   */
  @SuppressWarnings("unchecked")
  private static <T> T newInstance(Class<?> type) {
    try {
      return (T) type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ServiceException("Cannot instantiate service class " + type, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> Collection<T> getTypes(Class<T> type) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (Collection<T>) types.values() : Collections.emptyList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> T getType(Class<T> type, String name) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (T) types.get(name) : null;
  }
}
