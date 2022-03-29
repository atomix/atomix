// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import io.atomix.utils.Builder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive protocol builder.
 */
public abstract class PrimitiveProtocolBuilder<B extends PrimitiveProtocolBuilder<B, C, P>, C extends PrimitiveProtocolConfig<C>, P extends PrimitiveProtocol> implements Builder<P> {
  protected final C config;

  protected PrimitiveProtocolBuilder(C config) {
    this.config = checkNotNull(config);
  }
}
