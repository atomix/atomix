// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.primitive.SyncPrimitive;

/**
 * Generator for globally unique numeric identifiers.
 */
public interface AtomicIdGenerator extends SyncPrimitive {

  /**
   * Gets the next globally unique numeric identifier.
   *
   * @return the next globally unique numeric identifier
   */
  long nextId();

  @Override
  AsyncAtomicIdGenerator async();
}
