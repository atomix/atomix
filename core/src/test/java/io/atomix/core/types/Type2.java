// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.types;

/**
 * Serializable type for use in tests.
 */
public class Type2 {
  private String value1;
  private int value2;

  public Type2(String value1, int value2) {
    this.value1 = value1;
    this.value2 = value2;
  }

  public String value1() {
    return value1;
  }

  public int value2() {
    return value2;
  }
}
