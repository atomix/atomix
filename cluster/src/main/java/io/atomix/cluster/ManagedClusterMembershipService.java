// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.Managed;

/**
 * Managed cluster.
 */
public interface ManagedClusterMembershipService extends ClusterMembershipService, Managed<ClusterMembershipService> {
}
