/*
 * Copyright 2015-present Open Networking Foundation
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
 * limitations under the License
 */
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
