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

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation response.
 */
public class AppendResponse extends LogResponse {

  public static AppendResponse ok(long index) {
    return new AppendResponse(Status.OK, index);
  }

  public static AppendResponse error() {
    return new AppendResponse(Status.ERROR, 0);
  }

  private final long index;

  private AppendResponse(Status status, long index) {
    super(status);
    this.index = index;
  }

  public long index() {
    return index;
  }

  @Override
  public String toString() {
    if (status() == Status.OK) {
      return toStringHelper(this)
          .add("status", status())
          .add("index", index())
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status())
          .toString();
    }
  }
}
