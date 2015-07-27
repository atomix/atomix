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
package net.kuujo.copycat;

/**
 * Base type for Raft server errors.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftError {

  /**
   * Returns the Raft error for the given identifier.
   *
   * @param id The Raft error identifier.
   * @return The Raft error for the given identifier.
   * @throws IllegalArgumentException If the given identifier is not a valid Raft error identifier.
   */
  static RaftError forId(int id) {
    switch (id) {
      case 1:
        return Type.NO_LEADER_ERROR;
      case 2:
        return Type.QUERY_ERROR;
      case 3:
        return Type.COMMAND_ERROR;
      case 4:
        return Type.APPLICATION_ERROR;
      case 5:
        return Type.ILLEGAL_MEMBER_STATE_ERROR;
      case 6:
        return Type.UNKNOWN_SESSION_ERROR;
      case 7:
        return Type.INTERNAL_ERROR;
      default:
        throw new IllegalArgumentException("invalid error identifier: " + id);
    }
  }

  /**
   * Returns the unique error identifier.
   *
   * @return The unique error identifier.
   */
  byte id();

  /**
   * Creates a new exception for the error.
   *
   * @return The error exception.
   */
  RaftException createException();

  /**
   * Raft error types.
   */
  public static enum Type implements RaftError {

    /**
     * No leader error.
     */
    NO_LEADER_ERROR(1) {
      @Override
      public RaftException createException() {
        return new NoLeaderException("not the leader");
      }
    },

    /**
     * Read application error.
     */
    QUERY_ERROR(2) {
      @Override
      public RaftException createException() {
        return new ReadException("failed to obtain read quorum");
      }
    },

    /**
     * Write application error.
     */
    COMMAND_ERROR(3) {
      @Override
      public RaftException createException() {
        return new WriteException("failed to obtain write quorum");
      }
    },

    /**
     * User application error.
     */
    APPLICATION_ERROR(4) {
      @Override
      public RaftException createException() {
        return new ApplicationException("an application error occurred");
      }
    },

    /**
     * Illegal member state error.
     */
    ILLEGAL_MEMBER_STATE_ERROR(5) {
      @Override
      public RaftException createException() {
        return new IllegalMemberStateException("illegal member state");
      }
    },

    /**
     * Unknown session error.
     */
    UNKNOWN_SESSION_ERROR(6) {
      @Override
      public RaftException createException() {
        return new UnknownSessionException("unknown member session");
      }
    },

    /**
     * Internal error.
     */
    INTERNAL_ERROR(7) {
      @Override
      public RaftException createException() {
        return new InternalException("internal Raft error");
      }
    };

    private final byte id;

    private Type(int id) {
      this.id = (byte) id;
    }

    @Override
    public byte id() {
      return id;
    }
  }

}
