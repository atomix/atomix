// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import io.atomix.core.AtomixConfig;

/**
 * Client profile.
 */
public class ClientProfile implements Profile {
  public static final Type TYPE = new Type();

  /**
   * Client profile type.
   */
  public static class Type implements Profile.Type<ClientProfileConfig> {
    private static final String NAME = "client";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public ClientProfileConfig newConfig() {
      return new ClientProfileConfig();
    }

    @Override
    public Profile newProfile(ClientProfileConfig config) {
      return new ClientProfile();
    }
  }

  private final ClientProfileConfig config;

  ClientProfile() {
    this(new ClientProfileConfig());
  }

  ClientProfile(ClientProfileConfig config) {
    this.config = config;
  }

  @Override
  public ClientProfileConfig config() {
    return config;
  }

  @Override
  public void configure(AtomixConfig config) {
    // Do nothing! This profile is just for code readability.
  }
}
