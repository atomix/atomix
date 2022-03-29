// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.config;

/**
 * Typed configuration.
 */
public interface TypedConfig<T> extends Config {

  /**
   * Returns the type name.
   *
   * @return the type name
   */
  T getType();

}
