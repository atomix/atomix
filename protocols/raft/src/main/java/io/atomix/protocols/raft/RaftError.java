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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base type for Raft protocol errors.
 * <p>
 * Raft errors are passed on the wire in lieu of exceptions to reduce the overhead of serialization.
 * Each error is identifiable by an error ID which is used to serialize and deserialize errors.
 */
public class RaftError {
  private final Type type;
  private final String message;

  public RaftError(Type type, String message) {
    this.type = checkNotNull(type, "type cannot be null");
    this.message = message;
  }

  /**
   * Returns the error type.
   *
   * @return The error type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the error message.
   *
   * @return The error message.
   */
  public String message() {
    return message;
  }

  /**
   * Creates a new exception for the error.
   *
   * @return The error exception.
   */
  public RaftException createException() {
    return type.createException(message);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("message", message)
        .toString();
  }

  /**
   * Raft error types.
   */
  public enum Type {

    /**
     * No leader error.
     */
    NO_LEADER {
      @Override
      RaftException createException() {
        return createException("Failed to locate leader");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.NoLeader(message) : createException();
      }
    },

    /**
     * Read application error.
     */
    QUERY_FAILURE {
      @Override
      RaftException createException() {
        return createException("Failed to obtain read quorum");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.QueryFailure(message) : createException();
      }
    },

    /**
     * Write application error.
     */
    COMMAND_FAILURE {
      @Override
      RaftException createException() {
        return createException("Failed to obtain write quorum");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.CommandFailure(message) : createException();
      }
    },

    /**
     * User application error.
     */
    APPLICATION_ERROR {
      @Override
      RaftException createException() {
        return createException("An application error occurred");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.ApplicationException(message) : createException();
      }
    },

    /**
     * Illegal member state error.
     */
    ILLEGAL_MEMBER_STATE {
      @Override
      RaftException createException() {
        return createException("Illegal member state");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.IllegalMemberState(message) : createException();
      }
    },

    /**
     * Unknown client error.
     */
    UNKNOWN_CLIENT {
      @Override
      RaftException createException() {
        return createException("Unknown client");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.UnknownClient(message) : createException();
      }
    },

    /**
     * Unknown session error.
     */
    UNKNOWN_SESSION {
      @Override
      RaftException createException() {
        return createException("Unknown member session");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.UnknownSession(message) : createException();
      }
    },

    /**
     * Unknown state machine error.
     */
    UNKNOWN_SERVICE {
      @Override
      RaftException createException() {
        return createException("Unknown state machine");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.UnknownService(message) : createException();
      }
    },

    /**
     * Closed session error.
     */
    CLOSED_SESSION {
      @Override
      RaftException createException() {
        return createException("Closed session");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.ClosedSession(message) : createException();
      }
    },

    /**
     * Internal error.
     */
    PROTOCOL_ERROR {
      @Override
      RaftException createException() {
        return createException("Failed to reach consensus");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.ProtocolException(message) : createException();
      }
    },

    /**
     * Configuration error.
     */
    CONFIGURATION_ERROR {
      @Override
      RaftException createException() {
        return createException("Configuration failed");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.ConfigurationException(message) : createException();
      }
    },

    /**
     * Unavailable service error.
     */
    UNAVAILABLE {
      @Override
      RaftException createException() {
        return createException("Service is unavailable");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.Unavailable(message) : createException();
      }
    },

    /**
     * Read-only service error.
     */
    READ_ONLY {
      @Override
      RaftException createException() {
        return createException("Service is read-only");
      }

      @Override
      RaftException createException(String message) {
        return message != null ? new RaftException.ReadOnly(message) : createException();
      }
    };

    /**
     * Creates an exception with a default message.
     *
     * @return the exception
     */
    abstract RaftException createException();

    /**
     * Creates an exception with the given message.
     *
     * @param message the exception message
     * @return the exception
     */
    abstract RaftException createException(String message);
  }
}
