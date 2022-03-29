// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Work queue configuration.
 */
public class WorkQueueConfig extends PrimitiveConfig<WorkQueueConfig> {
  @Override
  public PrimitiveType getType() {
    return WorkQueueType.instance();
  }
}
