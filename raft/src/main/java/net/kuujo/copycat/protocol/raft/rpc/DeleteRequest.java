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
 * Protocol delete request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DeleteRequest extends CommandRequest<DeleteRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new delete request builder.
   *
   * @return A new delete request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a delete request builder for an existing request.
   *
   * @param request The request to build.
   * @return The delete request builder.
   */
  public static Builder builder(DeleteRequest request) {
    return builder.get().reset(request);
  }

  public DeleteRequest(ReferenceManager<DeleteRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.DELETE;
  }

  /**
   * Delete request builder.
   */
  public static class Builder extends CommandRequest.Builder<Builder, DeleteRequest> {
    private Builder() {
      super(DeleteRequest::new);
    }
  }

}
