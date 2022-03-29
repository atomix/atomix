// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

/**
 * Primitive consistency model.
 */
public enum Consistency {

  /**
   * Linearizable consistency model.
   */
  LINEARIZABLE,

  /**
   * Sequential consistency model.
   */
  SEQUENTIAL,

  /**
   * Eventual consistency model.
   */
  EVENTUAL,

}
