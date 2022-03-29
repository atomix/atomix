// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract identifier backed by another value, e.g. string, int.
 */
public class AbstractIdentifier<T extends Comparable<T>> implements Identifier<T> {

  protected final T identifier; // backing identifier value

  /**
   * Constructor for serialization.
   */
  protected AbstractIdentifier() {
    this.identifier = null;
  }

  /**
   * Constructs an identifier backed by the specified value.
   *
   * @param value the backing value
   */
  protected AbstractIdentifier(T value) {
    this.identifier = checkNotNull(value, "Identifier cannot be null.");
  }

  /**
   * Returns the backing identifier value.
   *
   * @return identifier
   */
  public T id() {
    return identifier;
  }

  /**
   * Returns the hashcode of the identifier.
   *
   * @return hashcode
   */
  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  /**
   * Compares two device key identifiers for equality.
   *
   * @param obj to compare against
   * @return true if the objects are equal, false otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AbstractIdentifier) {
      AbstractIdentifier that = (AbstractIdentifier) obj;
      return this.getClass() == that.getClass()
          && Objects.equals(this.identifier, that.identifier);
    }
    return false;
  }

  /**
   * Returns a string representation of a DeviceKeyId.
   *
   * @return string
   */
  public String toString() {
    return identifier.toString();
  }

}
