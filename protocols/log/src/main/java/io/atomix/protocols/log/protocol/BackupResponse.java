// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Backup response.
 */
public class BackupResponse extends LogResponse {

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
