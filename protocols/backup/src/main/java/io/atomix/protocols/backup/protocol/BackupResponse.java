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
 * Operation response.
 */
public class BackupResponse extends PrimaryBackupResponse {

  public static BackupResponse ok() {
    return new BackupResponse(Status.OK);
  }

  public static BackupResponse error() {
    return new BackupResponse(Status.ERROR);
  }

  private BackupResponse(Status status) {
    super(status);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status())
        .toString();
  }
}
