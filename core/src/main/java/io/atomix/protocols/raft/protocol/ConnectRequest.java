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
package io.atomix.protocols.raft.protocol;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Connect client request.
 * <p>
 * Connect requests are sent by clients to specific servers when first establishing a connection.
 * Connections must be associated with a specific client and must be established
 * each time the client switches servers. A client may only be connected to a single server at any
 * given time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConnectRequest extends AbstractRaftRequest {

  /**
   * Returns a new connect client request builder.
   *
   * @return A new connect client request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long session;
  private final long connection;

  public ConnectRequest(long session, long connection) {
    this.session = session;
    this.connection = connection;
  }

  @Override
  public Type type() {
    return Type.CONNECT;
  }

  /**
   * Returns the connecting session ID.
   *
   * @return The connecting session ID.
   */
  public long session() {
    return session;
  }

  /**
   * Returns the connection ID.
   *
   * @return The connection ID.
   */
  public long connection() {
    return connection;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ConnectRequest && ((ConnectRequest) object).session == session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
            .add("session", session)
            .add("connection", connection)
            .toString();
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, ConnectRequest> {
    private long session;
    private long connection;

    /**
     * Sets the connecting session ID.
     *
     * @param session The connecting session ID.
     * @return The connect request builder.
     */
    public Builder withSession(long session) {
      checkArgument(session > 0, "session must be positive");
      this.session = session;
      return this;
    }

    /**
     * Sets the connection ID.
     *
     * @param connection The connection ID.
     * @return The connect request builder.
     */
    public Builder withConnection(long connection) {
      checkArgument(connection > 0, "connection must be positive");
      this.connection = connection;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(session > 0, "session must be positive");
      checkArgument(connection > 0, "connection must be positive");
    }

    @Override
    public ConnectRequest build() {
      validate();
      return new ConnectRequest(session, connection);
    }
  }
}
