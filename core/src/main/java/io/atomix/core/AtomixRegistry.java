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
package io.atomix.core;

import io.atomix.core.impl.ClasspathScanningAtomixRegistry;
import io.atomix.core.profile.ProfileRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;

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
    return new ClasspathScanningAtomixRegistry(classLoader);
  }

  /**
   * Returns the partition group types.
   *
   * @return the partition group types
   */
  PartitionGroupTypeRegistry partitionGroupTypes();

  /**
   * Returns the primitive types.
   *
   * @return the primitive types
   */
  PrimitiveTypeRegistry primitiveTypes();

  /**
   * Returns the primitive protocol types.
   *
   * @return the primitive protocol types
   */
  PrimitiveProtocolTypeRegistry protocolTypes();

  /**
   * Returns the registered profile types.
   *
   * @return the registered profile types
   */
  ProfileRegistry profiles();

}
