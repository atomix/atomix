/*
 * Copyright 2016-present Open Networking Foundation
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
import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Collection;

/**
 * Server configuration change response.
 */
public class ReconfigureResponse extends ConfigurationResponse {

  /**
   * Returns a new reconfigure response builder.
   *
   * @return A new reconfigure response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public ReconfigureResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
    super(status, error, index, term, timestamp, members);
  }

  /**
   * Reconfigure response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<Builder, ReconfigureResponse> {
    @Override
    public ReconfigureResponse build() {
      validate();
      return new ReconfigureResponse(status, error, index, term, timestamp, members);
    }
  }
}
