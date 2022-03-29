// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

/**
 * Abstract identifier backed by another value, e.g. string, int.
 */
public interface Identifier<T extends Comparable<T>> {

  /**
   * Returns the backing identifier value.
   *
   * @return identifier
   */
  T id();
}
