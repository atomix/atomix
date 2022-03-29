// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import io.atomix.cluster.MemberId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Reset request.
 */
public class ResetRequest extends LogRequest {

  public static ResetRequest request(MemberId memberId, String subject, long index) {
    return new ResetRequest(memberId, subject, index);
  }

  private final MemberId memberId;
  private final String subject;
  private final long index;

  private ResetRequest(MemberId memberId, String subject, long index) {
    this.memberId = memberId;
    this.subject = subject;
    this.index = index;
  }

  public MemberId memberId() {
    return memberId;
  }

  public String subject() {
    return subject;
  }

  public long index() {
    return index;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("memberId", memberId())
        .add("subject", subject())
        .add("index", index())
        .toString();
  }
}
