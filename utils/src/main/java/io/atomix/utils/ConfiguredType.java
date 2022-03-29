// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import io.atomix.utils.config.TypedConfig;

/**
 * Configured type.
 */
public interface ConfiguredType<C extends TypedConfig> extends NamedType {

  /**
   * Returns a new configuration.
   *
   * @return a new configuration
   */
  C newConfig();

}
