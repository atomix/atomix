// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * A logical timestamp that derives its value from two input values. The first
 * value always takes precedence over the second value when comparing timestamps.
 */
public class MultiValuedTimestamp<T extends Comparable<T>, U extends Comparable<U>> implements Timestamp {
  private final T value1;
  private final U value2;

  /**
   * Creates a new timestamp based on two values. The first value has higher
   * precedence than the second when comparing timestamps.
   *
   * @param value1 first value
   * @param value2 second value
   */
  public MultiValuedTimestamp(T value1, U value2) {
    this.value1 = Preconditions.checkNotNull(value1);
    this.value2 = Preconditions.checkNotNull(value2);
  }

  @Override
  public int compareTo(Timestamp o) {
    Preconditions.checkArgument(o instanceof MultiValuedTimestamp,
        "Must be MultiValuedTimestamp", o);
    MultiValuedTimestamp that = (MultiValuedTimestamp) o;

    return ComparisonChain.start()
        .compare(this.value1, that.value1)
        .compare(this.value2, that.value2)
        .result();
  }

  @Override
  public int hashCode() {
    return Objects.hash(value1, value2);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MultiValuedTimestamp)) {
      return false;
    }
    MultiValuedTimestamp that = (MultiValuedTimestamp) obj;
    return Objects.equals(this.value1, that.value1)
        && Objects.equals(this.value2, that.value2);
  }

  @Override
  public String toString() {
    return toStringHelper(getClass())
        .add("value1", value1)
        .add("value2", value2)
        .toString();
  }

  /**
   * Returns the first value.
   *
   * @return first value
   */
  public T value1() {
    return value1;
  }

  /**
   * Returns the second value.
   *
   * @return second value
   */
  public U value2() {
    return value2;
  }

  // Default constructor for serialization
  @SuppressWarnings("unused")
  private MultiValuedTimestamp() {
    this.value1 = null;
    this.value2 = null;
  }
}
