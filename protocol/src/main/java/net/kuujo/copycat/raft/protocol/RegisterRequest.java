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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;

import java.util.Objects;
import java.util.UUID;

/**
 * Protocol register client request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=272)
public class RegisterRequest extends ClientRequest<RegisterRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new register client request builder.
   *
   * @return A new register client request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a register client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The register client request builder.
   */
  public static Builder builder(RegisterRequest request) {
    return builder.get().reset(request);
  }

  private int member;
  private UUID connection;

  public RegisterRequest(ReferenceManager<RegisterRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.REGISTER;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int member() {
    return member;
  }

  /**
   * Returns the connection ID.
   *
   * @return The connection ID.
   */
  public UUID connection() {
    return connection;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat serializer) {
    buffer.writeInt(member);
    serializer.writeObject(connection, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat serializer) {
    member = buffer.readInt();
    connection = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s", getClass().getSimpleName());
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends ClientRequest.Builder<Builder, RegisterRequest> {

    private Builder() {
      super(RegisterRequest::new);
    }

    @Override
    Builder reset() {
      super.reset();
      request.member = 0;
      request.connection = null;
      return this;
    }

    /**
     * Sets the member ID.
     *
     * @param member The member ID.
     * @return The request builder.
     */
    public Builder withMember(int member) {
      request.member = member;
      return this;
    }

    /**
     * Sets the connection ID.
     *
     * @param connection The connection ID.
     * @return The request builder.
     */
    public Builder withConnection(UUID connection) {
      request.connection = connection;
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
