// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import com.google.common.collect.ComparisonChain;
import io.atomix.utils.Identifier;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Vector clock timestamp.
 */
public class VectorTimestamp<T extends Identifier> extends LogicalTimestamp {
  private final T identifier;

  public VectorTimestamp(T identifier, long value) {
    super(value);
    this.identifier = identifier;
  }

  /**
   * Returns the timestamp identifier.
   *
   * @return the timestamp identifier
   */
  public T identifier() {
    return identifier;
  }

  @Override
  public int compareTo(Timestamp o) {
    checkArgument(o instanceof VectorTimestamp, "Must be VectorTimestamp", o);
    VectorTimestamp that = (VectorTimestamp) o;

    return ComparisonChain.start()
        .compare(this.identifier.id(), that.identifier.id())
        .compare(this.value(), that.value())
        .result();
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier(), value());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof VectorTimestamp)) {
      return false;
    }
    VectorTimestamp that = (VectorTimestamp) obj;
    return Objects.equals(this.identifier, that.identifier)
        && Objects.equals(this.value(), that.value());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("identifier", identifier())
        .add("value", value())
        .toString();
  }
}
