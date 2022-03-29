// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.utils.Managed;

/**
 * Managed transaction service.
 */
public interface ManagedTransactionService extends TransactionService, Managed<TransactionService> {
}
