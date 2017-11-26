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

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation request.
 */
public class CloseRequest extends PrimitiveRequest {

  public static CloseRequest request(PrimitiveDescriptor primitive, long session) {
    return new CloseRequest(primitive, session);
  }

  private final long session;

  public CloseRequest(PrimitiveDescriptor primitive, long session) {
    super(primitive);
    this.session = session;
  }

  public long session() {
    return session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive())
        .add("session", session())
        .toString();
  }
}
