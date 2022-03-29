// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore.impl;

import io.atomix.primitive.event.Event;

/**
 * Distributed semaphore client.
 */
public interface AtomicSemaphoreClient {

  @Event("succeeded")
  void succeeded(long id, long version, int permits);

  @Event("failed")
  void failed(long id);

}
