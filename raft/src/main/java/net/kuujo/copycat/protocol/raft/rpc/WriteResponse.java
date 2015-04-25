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
 * Protocol write response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WriteResponse extends CommandResponse<WriteResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new write response builder.
   *
   * @return A new write response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a write response builder for an existing response.
   *
   * @param response The response to build.
   * @return The write response builder.
   */
  public static Builder builder(WriteResponse response) {
    return builder.get().reset(response);
  }

  public WriteResponse(ReferenceManager<WriteResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.WRITE;
  }

  /**
   * Write response builder.
   */
  public static class Builder extends CommandResponse.Builder<Builder, WriteResponse> {
    private Builder() {
      super(WriteResponse::new);
    }
  }

}
