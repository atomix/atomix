// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Distributed log configuration.
 */
public class DistributedLogConfig extends PrimitiveConfig<DistributedLogConfig> {
  @Override
  public PrimitiveType getType() {
    return DistributedLogType.instance();
  }
}
