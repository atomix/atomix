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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Restore request.
 */
public class RestoreRequest extends PrimitiveRequest {

  public static RestoreRequest request(PrimitiveDescriptor primitive, MemberId memberId, long term) {
    return new RestoreRequest(primitive, memberId, term);
  }

  private final long term;
  private final MemberId memberId;

  public RestoreRequest(PrimitiveDescriptor primitive, MemberId memberId, long term) {
    super(primitive);
    this.term = term;
    this.memberId = memberId;
  }

  public long term() {
    return term;
  }

  public MemberId memberId() {
    return memberId;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive())
        .add("memberId", memberId())
        .add("term", term())
        .toString();
  }
}
