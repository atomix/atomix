// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * Leader election configuration.
 */
public class LeaderElectionConfig extends PrimitiveConfig<LeaderElectionConfig> {
  @Override
  public PrimitiveType getType() {
    return LeaderElectionType.instance();
  }
}
