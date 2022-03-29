// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

/**
 * Describes the order of a primitive data structure.
 */
public enum Ordering {

  /**
   * Indicates that items should be ordered in their natural order.
   */
  NATURAL,

  /**
   * Indicates that items should be ordered in insertion order.
   */
  INSERTION,
}
