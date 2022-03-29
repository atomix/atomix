// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.counter;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;

/**
 * Counter protocol.
 */
@Beta
public interface CounterProtocol extends GossipProtocol {

  /**
   * Returns a new counter delegate.
   *
   * @param name the counter name
   * @param managementService the primitive management service
   * @return a new counter delegate
   */
  CounterDelegate newCounterDelegate(String name, PrimitiveManagementService managementService);

}
