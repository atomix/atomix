// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.core.cache.CachedPrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Leader elector configuration.
 */
public class LeaderElectorConfig extends CachedPrimitiveConfig<LeaderElectorConfig> {
  @Override
  public PrimitiveType getType() {
    return LeaderElectorType.instance();
  }
}
