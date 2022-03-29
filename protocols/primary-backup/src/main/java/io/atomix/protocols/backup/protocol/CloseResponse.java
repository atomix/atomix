// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

/**
 * Operation response.
 */
public class CloseResponse extends PrimaryBackupResponse {

  public static CloseResponse ok() {
    return new CloseResponse(Status.OK);
  }

  public static CloseResponse error() {
    return new CloseResponse(Status.ERROR);
  }

  private CloseResponse(Status status) {
    super(status);
  }
}
