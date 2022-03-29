// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging;

import io.atomix.utils.Managed;

/**
 * Managed broadcast service.
 */
public interface ManagedBroadcastService extends BroadcastService, Managed<BroadcastService> {
}
