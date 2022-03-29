// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

/**
 * Abstract named object.
 */
public abstract class AbstractNamed implements Named {
  private String name;

  protected AbstractNamed() {
    this(null);
  }

  protected AbstractNamed(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }
}
