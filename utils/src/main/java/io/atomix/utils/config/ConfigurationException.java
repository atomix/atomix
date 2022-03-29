// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.config;

import io.atomix.utils.AtomixRuntimeException;

/**
 * Atomix configuration exception.
 */
public class ConfigurationException extends AtomixRuntimeException {
  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
