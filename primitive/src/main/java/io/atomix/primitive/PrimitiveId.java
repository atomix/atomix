// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import com.google.common.hash.Hashing;
import io.atomix.utils.AbstractIdentifier;

import java.nio.charset.StandardCharsets;

/**
 * Snapshot identifier.
 */
public class PrimitiveId extends AbstractIdentifier<Long> {

  /**
   * Creates a snapshot ID from the given number.
   *
   * @param id the number from which to create the identifier
   * @return the snapshot identifier
   */
  public static PrimitiveId from(long id) {
    return new PrimitiveId(id);
  }

  /**
   * Creates a snapshot ID from the given string.
   *
   * @param id the string from which to create the identifier
   * @return the snapshot identifier
   */
  public static PrimitiveId from(String id) {
    return from(Hashing.sha256().hashString(id, StandardCharsets.UTF_8).asLong());
  }

  public PrimitiveId(Long value) {
    super(value);
  }
}
