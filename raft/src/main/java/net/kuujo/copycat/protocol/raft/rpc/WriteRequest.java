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
 * Protocol write request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WriteRequest extends CommandRequest<WriteRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new write request builder.
   *
   * @return A new write request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a write request builder for an existing request.
   *
   * @param request The request to build.
   * @return The write request builder.
   */
  public static Builder builder(WriteRequest request) {
    return builder.get().reset(request);
  }

  public WriteRequest(ReferenceManager<WriteRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.WRITE;
  }

  /**
   * Write request builder.
   */
  public static class Builder extends CommandRequest.Builder<Builder, WriteRequest> {
    private Builder() {
      super(WriteRequest::new);
    }
  }

}
