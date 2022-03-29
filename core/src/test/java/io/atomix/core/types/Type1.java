// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.types;

/**
 * Serializable type for use in tests.
 */
public class Type1 {
  String value;

  public Type1(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }
}
