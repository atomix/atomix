// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import static io.atomix.core.profile.ClientProfile.TYPE;

/**
 * Client profile configuration.
 */
public class ClientProfileConfig extends ProfileConfig {
  @Override
  public Profile.Type getType() {
    return TYPE;
  }
}
