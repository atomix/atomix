// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.core.value.AtomicValueType;

/**
 * Default atomic value service.
 */
public class DefaultAtomicValueService extends AbstractAtomicValueService {
  public DefaultAtomicValueService() {
    super(AtomicValueType.instance());
  }
}
