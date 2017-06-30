/*
 * Copyright 2015-present Open Networking Laboratory
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
 * Base type for Raft protocol errors.
 * <p>
 * Raft errors are passed on the wire in lieu of exceptions to reduce the overhead of serialization.
 * Each error is identifiable by an error ID which is used to serialize and deserialize errors.
 */
public interface RaftError {

  /**
   * Creates a new exception for the error.
   *
   * @return The error exception.
   */
  RaftException createException();

  /**
   * Creates a new exception for the error.
   *
   * @param message The exception message.
   * @return The exception.
   */
  RaftException createException(String message);

  /**
   * Raft error types.
   */
  enum Type implements RaftError {

    /**
     * No leader error.
     */
    NO_LEADER {
      @Override
      public RaftException createException() {
        return createException("not the leader");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.NoLeader(message);
      }
    },

    /**
     * Read application error.
     */
    QUERY_FAILURE {
      @Override
      public RaftException createException() {
        return createException("failed to obtain read quorum");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.QueryFailure(message);
      }
    },

    /**
     * Write application error.
     */
    COMMAND_FAILURE {
      @Override
      public RaftException createException() {
        return createException("Failed to obtain write quorum");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.CommandFailure(message);
      }
    },

    /**
     * User application error.
     */
    APPLICATION_ERROR {
      @Override
      public RaftException createException() {
        return createException("An application error occurred");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.ApplicationException(message);
      }
    },

    /**
     * Illegal member state error.
     */
    ILLEGAL_MEMBER_STATE {
      @Override
      public RaftException createException() {
        return createException("Illegal member state");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.IllegalMemberState(message);
      }
    },

    /**
     * Unknown client error.
     */
    UNKNOWN_CLIENT {
      @Override
      public RaftException createException() {
        return createException("Unknown client");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.UnknownClient(message);
      }
    },

    /**
     * Unknown session error.
     */
    UNKNOWN_SESSION {
      @Override
      public RaftException createException() {
        return createException("Unknown member session");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.UnknownSession(message);
      }
    },

    /**
     * Unknown state machine error.
     */
    UNKNOWN_SERVICE {
      @Override
      public RaftException createException() {
        return createException("Unknown state machine");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.UnknownService(message);
      }
    },

    /**
     * Closed session error.
     */
    CLOSED_SESSION {
      @Override
      public RaftException createException() {
        return createException("Closed session");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.ClosedSession(message);
      }
    },

    /**
     * Internal error.
     */
    PROTOCOL_ERROR {
      @Override
      public RaftException createException() {
        return createException("Internal Raft error");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.ProtocolException(message);
      }
    },

    /**
     * Configuration error.
     */
    CONFIGURATION_ERROR {
      @Override
      public RaftException createException() {
        return createException("Configuration failed");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.ConfigurationException(message);
      }
    },

    /**
     * Unavailable service error.
     */
    UNAVAILABLE {
      @Override
      public RaftException createException() {
        return createException("Service is unavailable");
      }

      @Override
      public RaftException createException(String message) {
        return new RaftException.Unavailable(message);
      }
    }
  }
}
