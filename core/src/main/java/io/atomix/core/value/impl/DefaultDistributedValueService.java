// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.core.value.DistributedValueType;

/**
 * Distributed value service implementation.
 */
public class DefaultDistributedValueService extends AbstractAtomicValueService {
  public DefaultDistributedValueService() {
    super(DistributedValueType.instance());
  }
}
