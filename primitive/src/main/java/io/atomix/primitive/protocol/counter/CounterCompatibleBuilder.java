// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.counter;

import com.google.common.annotations.Beta;

/**
 * Counter builder.
 */
@Beta
public interface CounterCompatibleBuilder<B extends CounterCompatibleBuilder<B>> {

  /**
   * Configures the builder with a counter compatible gossip protocol.
   *
   * @param protocol the counter protocol
   * @return the primitive builder
   */
  B withProtocol(CounterProtocol protocol);

}
