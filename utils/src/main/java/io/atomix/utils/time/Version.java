// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import com.google.common.collect.ComparisonChain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Logical timestamp for versions.
 * <p>
 * The version is a logical timestamp that represents a point in logical time at which an event occurs.
 * This is used in both pessimistic and optimistic locking protocols to ensure that the state of a shared resource
 * has not changed at the end of a transaction.
 */
public class Version implements Timestamp {
  private final long version;

  public Version(long version) {
    this.version = version;
  }

  /**
   * Returns the version.
   *
   * @return the version
   */
  public long value() {
    return this.version;
  }

  @Override
  public int compareTo(Timestamp o) {
    checkArgument(o instanceof Version,
        "Must be LockVersion", o);
    Version that = (Version) o;

    return ComparisonChain.start()
        .compare(this.version, that.version)
        .result();
  }

  @Override
  public int hashCode() {
    return Long.hashCode(version);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Version)) {
      return false;
    }
    Version that = (Version) obj;
    return Objects.equals(this.version, that.version);
  }

  @Override
  public String toString() {
    return toStringHelper(getClass())
        .add("version", version)
        .toString();
  }
}
