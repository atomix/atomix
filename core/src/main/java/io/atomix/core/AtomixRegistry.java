// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core;

import io.atomix.core.registry.ClasspathScanningRegistry;
import io.atomix.utils.NamedType;

import java.util.Collection;

/**
 * Atomix registry.
 */
public interface AtomixRegistry {

  /**
   * Creates a new registry.
   *
   * @return the registry instance
   */
  static AtomixRegistry registry() {
    return registry(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a new registry instance using the given class loader.
   *
   * @param classLoader the registry class loader
   * @return the registry instance
   */
  static AtomixRegistry registry(ClassLoader classLoader) {
    return ClasspathScanningRegistry.builder().withClassLoader(classLoader).build();
  }

  /**
   * Returns the collection of registrations for the given type.
   *
   * @param type the type for which to return registrations
   * @param <T>  the type for which to return registrations
   * @return a collection of registrations for the given type
   */
  <T extends NamedType> Collection<T> getTypes(Class<T> type);

  /**
   * Returns a named registration by type.
   *
   * @param type the registration type
   * @param name the registration name
   * @param <T>  the registration type
   * @return the registration instance
   */
  <T extends NamedType> T getType(Class<T> type, String name);

}
