// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.utils.Managed;

/**
 * Managed node discovery service.
 */
public interface ManagedNodeDiscoveryService extends NodeDiscoveryService, Managed<NodeDiscoveryService> {
}
