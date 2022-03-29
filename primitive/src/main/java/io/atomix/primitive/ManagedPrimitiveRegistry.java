// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import io.atomix.utils.Managed;

/**
 * Managed primitive registry.
 */
public interface ManagedPrimitiveRegistry extends PrimitiveRegistry, Managed<PrimitiveRegistry> {
}
