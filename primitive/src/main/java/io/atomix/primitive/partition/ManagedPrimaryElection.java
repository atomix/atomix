// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.Managed;

/**
 * Managed primary election.
 */
public interface ManagedPrimaryElection extends PrimaryElection, Managed<PrimaryElection> {
}
