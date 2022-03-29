// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.config;

/**
 * Named configuration.
 */
public interface NamedConfig<C extends NamedConfig<C>> extends Config {

  /**
   * Returns the configuration name.
   *
   * @return the configuration name
   */
  String getName();

  /**
   * Sets the configuration name.
   *
   * @param name the configuration name
   * @return the configuration object
   */
  C setName(String name);

}
