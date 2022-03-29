// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

/**
 * Cluster metadata request.
 */
public class MetadataRequest extends SessionRequest {

  /**
   * Returns a new metadata request builder.
   *
   * @return A new metadata request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public MetadataRequest(long session) {
    super(session);
  }

  /**
   * Metadata request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, MetadataRequest> {
    @Override
    public MetadataRequest build() {
      return new MetadataRequest(session);
    }
  }
}
