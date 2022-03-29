// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.AbstractIdentifier;

/**
 * Member group identifier.
 */
public class MemberGroupId extends AbstractIdentifier<String> {

  /**
   * Creates a new member group identifier.
   *
   * @param id the group ID
   * @return the new member group identifier
   */
  public static MemberGroupId from(String id) {
    return new MemberGroupId(id);
  }

  private MemberGroupId() {
  }

  public MemberGroupId(String value) {
    super(value);
  }
}
