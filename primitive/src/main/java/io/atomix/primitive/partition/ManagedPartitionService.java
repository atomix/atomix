// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.Managed;

/**
 * Managed partition service.
 */
public interface ManagedPartitionService extends PartitionService, Managed<PartitionService> {
}
