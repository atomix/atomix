// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Atomix primitive cache.
 */
public interface PrimitiveCache {

  /**
   * Gets or creates a locally cached multiton primitive instance.
   *
   * @param name the primitive name
   * @param supplier the primitive factory
   * @param <P> the primitive type
   * @return the primitive instance
   */
  <P extends DistributedPrimitive> CompletableFuture<P> getPrimitive(String name, Supplier<CompletableFuture<P>> supplier);

}
