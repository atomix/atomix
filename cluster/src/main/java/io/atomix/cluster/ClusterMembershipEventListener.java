// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.event.EventListener;

/**
 * Entity capable of receiving device cluster-related events.
 */
public interface ClusterMembershipEventListener extends EventListener<ClusterMembershipEvent> {
}
