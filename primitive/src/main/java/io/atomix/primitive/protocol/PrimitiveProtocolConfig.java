// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import io.atomix.utils.config.TypedConfig;

/**
 * Primitive protocol configuration.
 */
public abstract class PrimitiveProtocolConfig<C extends PrimitiveProtocolConfig<C>> implements TypedConfig<PrimitiveProtocol.Type> {
}
