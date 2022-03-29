// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree.impl;

import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.primitive.event.Event;

/**
 * Document tree client.
 */
public interface DocumentTreeClient {

  /**
   * Handles a document tree event.
   *
   * @param event the document tree event
   */
  @Event
  void change(DocumentTreeEvent<byte[]> event);

}
