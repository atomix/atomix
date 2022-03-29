// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.config;

/**
 * Interface for objects configured via a configuration object.
 */
public interface Configured<T extends Config> {

  /**
   * Returns the object configuration.
   *
   * @return the object configuration
   */
  T config();

}
