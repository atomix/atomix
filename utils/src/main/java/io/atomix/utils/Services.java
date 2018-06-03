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
package io.atomix.utils;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Service utilities.
 */
public final class Services {

  /**
   * Loads a collection of services of a given type from the classpath.
   *
   * @param type        the service type
   * @param classLoader the service class loader
   * @param <T>         the service type
   * @return a collection of services of the given type on the classpath
   */
  @SuppressWarnings("unchecked")
  public static <T> Collection<T> loadTypes(Class<T> type, ClassLoader classLoader) {
    if (type.isInterface()) {
      return loadAllByInterface(type, classLoader);
    } else {
      return loadAllByClass(type, classLoader);
    }
  }

  /**
   * Loads a collection of services of a given class from the classpath.
   *
   * @param clazz       the service class
   * @param classLoader the service class loader
   * @param <T>         the service class
   * @return a collection of services of the given class on the classpath
   */
  @SuppressWarnings("unchecked")
  private static <T> Collection<T> loadAllByClass(Class<T> clazz, ClassLoader classLoader) {
    ScanResult result = scan(classLoader);
    return result.getClassNameToClassInfo()
        .get(clazz.getName())
        .getSubclasses()
        .stream()
        .filter(classInfo -> !classInfo.isAbstract())
        .map(classInfo -> Services.<T>newInstance(classInfo.getClassRef()))
        .collect(Collectors.toList());
  }

  /**
   * Loads a collection of services of a given interface from the classpath.
   *
   * @param iface       the service interface
   * @param classLoader the service class loader
   * @param <T>         the service interface
   * @return a collection of services of the given interface on the classpath
   */
  @SuppressWarnings("unchecked")
  private static <T> Collection<T> loadAllByInterface(Class<T> iface, ClassLoader classLoader) {
    ScanResult result = scan(classLoader);
    return result.getClassNameToClassInfo()
        .get(iface.getName())
        .getClassesImplementing()
        .stream()
        .filter(classInfo -> !classInfo.isAbstract())
        .map(classInfo -> Services.<T>newInstance(classInfo.getClassRef()))
        .collect(Collectors.toList());
  }

  /**
   * Scans the classpath using the given class loader to load classes.
   *
   * @param classLoader the service class loader
   * @return the scan result
   */
  private static ScanResult scan(ClassLoader classLoader) {
    return new FastClasspathScanner().addClassLoader(classLoader).scan();
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

  private Services() {
  }
}
