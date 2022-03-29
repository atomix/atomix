// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import java.util.Collection;

/**
 * Primitive registry.
 */
public interface PrimitiveTypeRegistry {

  /**
   * Returns the collection of registered primitive types.
   *
   * @return the collection of registered primitive types
   */
  Collection<PrimitiveType> getPrimitiveTypes();

  /**
   * Returns the primitive type for the given name.
   *
   * @param typeName the primitive type name
   * @return the primitive type
   */
  PrimitiveType getPrimitiveType(String typeName);

}
