// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue.impl;

import io.atomix.primitive.event.Event;

/**
 * Work queue client.
 */
public interface WorkQueueClient {

  /**
   * Notifies the client that a task is available.
   */
  @Event
  void taskAvailable();

}
