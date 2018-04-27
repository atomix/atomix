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
 * limitations under the License.
 */
package io.atomix.protocols.raft;

/**
 * Base Raft protocol exception.
 * <p>
 * This is the base exception type for all Raft protocol exceptions. Protocol exceptions must be
 * associated with a {@link RaftError.Type} which is used for more efficient serialization.
 */
public abstract class RaftException extends RuntimeException {
  private final RaftError.Type type;

  protected RaftException(RaftError.Type type, String message, Object... args) {
    super(message != null ? String.format(message, args) : null);
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  protected RaftException(RaftError.Type type, Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  protected RaftException(RaftError.Type type, Throwable cause) {
    super(cause);
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  /**
   * Returns the exception type.
   *
   * @return The exception type.
   */
  public RaftError.Type getType() {
    return type;
  }

  public static class NoLeader extends RaftException {
    public NoLeader(String message, Object... args) {
      super(RaftError.Type.NO_LEADER, message, args);
    }
  }

  public static class IllegalMemberState extends RaftException {
    public IllegalMemberState(String message, Object... args) {
      super(RaftError.Type.ILLEGAL_MEMBER_STATE, message, args);
    }
  }

  public static class ApplicationException extends RaftException {
    public ApplicationException(String message, Object... args) {
      super(RaftError.Type.APPLICATION_ERROR, message, args);
    }

    public ApplicationException(Throwable cause) {
      super(RaftError.Type.APPLICATION_ERROR, cause);
    }
  }

  public abstract static class OperationFailure extends RaftException {
    public OperationFailure(RaftError.Type type, String message, Object... args) {
      super(type, message, args);
    }
  }

  public static class CommandFailure extends OperationFailure {
    public CommandFailure(String message, Object... args) {
      super(RaftError.Type.COMMAND_FAILURE, message, args);
    }
  }

  public static class QueryFailure extends OperationFailure {
    public QueryFailure(String message, Object... args) {
      super(RaftError.Type.QUERY_FAILURE, message, args);
    }
  }

  public static class UnknownClient extends RaftException {
    public UnknownClient(String message, Object... args) {
      super(RaftError.Type.UNKNOWN_CLIENT, message, args);
    }
  }

  public static class UnknownSession extends RaftException {
    public UnknownSession(String message, Object... args) {
      super(RaftError.Type.UNKNOWN_SESSION, message, args);
    }
  }

  public static class UnknownService extends RaftException {
    public UnknownService(String message, Object... args) {
      super(RaftError.Type.UNKNOWN_SERVICE, message, args);
    }
  }

  public static class ClosedSession extends RaftException {
    public ClosedSession(String message, Object... args) {
      super(RaftError.Type.CLOSED_SESSION, message, args);
    }
  }

  public static class ProtocolException extends RaftException {
    public ProtocolException(String message, Object... args) {
      super(RaftError.Type.PROTOCOL_ERROR, message, args);
    }
  }

  public static class ConfigurationException extends RaftException {
    public ConfigurationException(String message, Object... args) {
      super(RaftError.Type.CONFIGURATION_ERROR, message, args);
    }
  }

  public static class Unavailable extends RaftException {
    public Unavailable(String message, Object... args) {
      super(RaftError.Type.UNAVAILABLE, message, args);
    }

    public Unavailable(Throwable cause) {
      super(RaftError.Type.UNAVAILABLE, cause);
    }
  }
}
