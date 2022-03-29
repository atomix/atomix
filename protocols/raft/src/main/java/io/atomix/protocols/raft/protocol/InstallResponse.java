// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

/**
 * Snapshot installation response.
 * <p>
 * Install responses are sent once a snapshot installation request has been received and processed.
 * Install responses provide no additional metadata aside from indicating whether or not the request
 * was successful.
 */
public class InstallResponse extends AbstractRaftResponse {

  /**
   * Returns a new install response builder.
   *
   * @return A new install response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public InstallResponse(Status status, RaftError error) {
    super(status, error);
  }

  /**
   * Install response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, InstallResponse> {
    @Override
    public InstallResponse build() {
      validate();
      return new InstallResponse(status, error);
    }
  }
}
