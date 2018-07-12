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
package io.atomix.primitive.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.utils.ServiceException;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classpath scanning primitive type registry.
 */
public class ClasspathScanningPrimitiveTypeRegistry implements PrimitiveTypeRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathScanningPrimitiveTypeRegistry.class);

  private final Map<String, PrimitiveType> primitiveTypes = new ConcurrentHashMap<>();

  public ClasspathScanningPrimitiveTypeRegistry(ClassLoader classLoader) {
    final String scanSpec = System.getProperty("io.atomix.scanSpec");
    final FastClasspathScanner classpathScanner = scanSpec != null ?
            new FastClasspathScanner(scanSpec).addClassLoader(classLoader) :
            new FastClasspathScanner().addClassLoader(classLoader);
    classpathScanner.matchClassesImplementing(PrimitiveType.class, type -> {
      if (!Modifier.isAbstract(type.getModifiers()) && !Modifier.isPrivate(type.getModifiers())) {
        PrimitiveType primitiveType = newInstance(type);
        PrimitiveType oldPrimitiveType = primitiveTypes.put(primitiveType.name(), primitiveType);
        if (oldPrimitiveType != null) {
          LOGGER.warn("Found multiple primitives types name={}, classes=[{}, {}]", primitiveType.name(),
                  oldPrimitiveType.getClass().getName(), primitiveType.getClass().getName());
        }
      }
    }).scan();
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
  public Collection<PrimitiveType> getPrimitiveTypes() {
    return primitiveTypes.values();
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    PrimitiveType type = primitiveTypes.get(typeName);
    if (type == null) {
      throw new ServiceException("Unknown primitive type " + typeName);
    }
    return type;
  }
}
