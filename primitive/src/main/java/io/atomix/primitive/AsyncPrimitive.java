// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous primitive.
 */
public interface AsyncPrimitive extends DistributedPrimitive {

  /**
   * Closes the primitive.
   *
   * @return a future to be completed once the primitive is closed
   */
  CompletableFuture<Void> close();

  /**
   * Purges state associated with this primitive.
   * <p>
   * Implementations can override and provide appropriate clean up logic for purging
   * any state state associated with the primitive. Whether modifications made within the
   * destroy method have local or global visibility is left unspecified.
   *
   * @return {@code CompletableFuture} that is completed when the operation completes
   */
  CompletableFuture<Void> delete();

  /**
   * Returns a synchronous wrapper around the asynchronous primitive.
   *
   * @return the synchronous primitive
   */
  SyncPrimitive sync();

  /**
   * Returns a synchronous wrapper around the asynchronous primitive.
   *
   * @param operationTimeout the synchronous operation timeout
   * @return the synchronous primitive
   */
  SyncPrimitive sync(Duration operationTimeout);

}
