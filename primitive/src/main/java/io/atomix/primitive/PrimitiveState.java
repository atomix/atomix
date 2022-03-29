// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

/**
 * State of distributed primitive.
 */
public enum PrimitiveState {

  /**
   * Signifies a state wherein the primitive is operating correctly and is capable of meeting the advertised
   * consistency and reliability guarantees.
   */
  CONNECTED,

  /**
   * Signifies a state wherein the primitive is temporarily incapable of providing the advertised
   * consistency properties.
   */
  SUSPENDED,

  /**
   * Signifies a state wherein the primitive's session has been expired and therefore cannot perform its functions.
   */
  EXPIRED,

  /**
   * Signifies a state wherein the primitive session has been closed and therefore cannot perform its functions.
   */
  CLOSED
}
