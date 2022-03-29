// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.utils.ServiceException;
import io.atomix.utils.misc.StringUtils;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classpath scanning primitive type registry.
 */
public class ClasspathScanningPrimitiveTypeRegistry implements PrimitiveTypeRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathScanningPrimitiveTypeRegistry.class);

  private static final Map<ClassLoader, Map<String, PrimitiveType>> CACHE =
          Collections.synchronizedMap(new WeakHashMap<>());

  private final Map<String, PrimitiveType> primitiveTypes = new ConcurrentHashMap<>();

  public ClasspathScanningPrimitiveTypeRegistry(ClassLoader classLoader) {
    Map<String, PrimitiveType> types = CACHE.computeIfAbsent(classLoader, cl -> {
      final Map<String, PrimitiveType> result = new ConcurrentHashMap<>();
      final String[] whitelistPackages = StringUtils.split(System.getProperty("io.atomix.whitelistPackages"), ",");
      final ClassGraph classGraph = whitelistPackages != null
          ? new ClassGraph().enableClassInfo().whitelistPackages(whitelistPackages).addClassLoader(classLoader)
          : new ClassGraph().enableClassInfo().addClassLoader(classLoader);
      try (final ScanResult scanResult = classGraph.scan()) {
        scanResult.getClassesImplementing(PrimitiveType.class.getName()).forEach(classInfo -> {
          if (classInfo.isInterface() || classInfo.isAbstract() || Modifier.isPrivate(classInfo.getModifiers())) {
            return;
          }
          final PrimitiveType primitiveType = newInstance(classInfo.loadClass());
          final PrimitiveType oldPrimitiveType = result.put(primitiveType.name(), primitiveType);
          if (oldPrimitiveType != null) {
            LOGGER.warn("Found multiple primitives types name={}, classes=[{}, {}]", primitiveType.name(),
                    oldPrimitiveType.getClass().getName(), primitiveType.getClass().getName());
          }
        });
      }
      return Collections.unmodifiableMap(result);
    });
    primitiveTypes.putAll(types);
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
