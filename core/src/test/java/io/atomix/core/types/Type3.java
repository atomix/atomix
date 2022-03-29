// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.types;

/**
 * Serializable type for use in tests.
 */
public class Type3 {
  int value;

  public Type3(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }
}
