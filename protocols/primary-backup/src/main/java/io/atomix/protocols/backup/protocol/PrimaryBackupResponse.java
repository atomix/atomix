// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

/**
 * Primary-backup response.
 */
public abstract class PrimaryBackupResponse {

  /**
   * Response status.
   */
  public enum Status {
    OK,
    ERROR,
  }

  private final Status status;

  public PrimaryBackupResponse(Status status) {
    this.status = status;
  }

  public Status status() {
    return status;
  }
}
