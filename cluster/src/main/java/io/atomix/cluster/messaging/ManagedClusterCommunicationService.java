// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging;

import io.atomix.utils.Managed;

/**
 * Managed cluster communicator.
 */
public interface ManagedClusterCommunicationService extends ClusterCommunicationService, Managed<ClusterCommunicationService> {
}
