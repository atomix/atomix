/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol.raft.rpc;

import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Protocol delete response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DeleteResponse extends CommandResponse<DeleteResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new delete response builder.
   *
   * @return A new delete response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a delete response builder for an existing response.
   *
   * @param response The response to build.
   * @return The delete response builder.
   */
  public static Builder builder(DeleteResponse response) {
    return builder.get().reset(response);
  }

  public DeleteResponse(ReferenceManager<DeleteResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.DELETE;
  }

  /**
   * Delete response builder.
   */
  public static class Builder extends CommandResponse.Builder<Builder, DeleteResponse> {
    private Builder() {
      super(DeleteResponse::new);
    }
  }

}
