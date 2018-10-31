/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
