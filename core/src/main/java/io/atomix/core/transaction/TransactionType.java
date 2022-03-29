// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.impl.CoreTransactionService;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transaction primitive type.
 */
public class TransactionType implements PrimitiveType<TransactionBuilder, TransactionConfig, Transaction> {
  private static final String NAME = "transaction";
  private static final TransactionType INSTANCE = new TransactionType();

  /**
   * Returns a new consistent tree map type.
   *
   * @return a new consistent tree map type
   */
  public static TransactionType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransactionConfig newConfig() {
    return new TransactionConfig();
  }

  @Override
  public TransactionBuilder newBuilder(String name, TransactionConfig config, PrimitiveManagementService managementService) {
    return new DefaultTransactionBuilder(name, config, managementService, new CoreTransactionService(managementService));
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
