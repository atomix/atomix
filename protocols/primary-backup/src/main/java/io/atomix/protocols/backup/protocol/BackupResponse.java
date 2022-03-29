// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
