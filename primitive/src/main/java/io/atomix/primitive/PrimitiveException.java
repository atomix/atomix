// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.primitive;

import io.atomix.utils.AtomixRuntimeException;

/**
 * Top level exception for Store failures.
 */
@SuppressWarnings("serial")
public class PrimitiveException extends AtomixRuntimeException {
  public PrimitiveException() {
  }

  public PrimitiveException(String message) {
    super(message);
  }

  public PrimitiveException(Throwable t) {
    super(t);
  }

  /**
   * Store is temporarily unavailable.
   */
  public static class Unavailable extends PrimitiveException {
    public Unavailable() {
    }

    public Unavailable(String message) {
      super(message);
    }
  }

  /**
   * Store operation timeout.
   */
  public static class Timeout extends PrimitiveException {
  }

  /**
   * Store update conflicts with an in flight transaction.
   */
  public static class ConcurrentModification extends PrimitiveException {
  }

  /**
   * Store operation interrupted.
   */
  public static class Interrupted extends PrimitiveException {
  }

  /**
   * Primitive service exception.
   */
  public static class ServiceException extends PrimitiveException {
    public ServiceException() {
    }

    public ServiceException(String message) {
      super(message);
    }

    public ServiceException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Command failure exception.
   */
  public static class CommandFailure extends PrimitiveException {
    public CommandFailure() {
    }

    public CommandFailure(String message) {
      super(message);
    }
  }

  /**
   * Query failure exception.
   */
  public static class QueryFailure extends PrimitiveException {
    public QueryFailure() {
    }

    public QueryFailure(String message) {
      super(message);
    }
  }

  /**
   * Unknown client exception.
   */
  public static class UnknownClient extends PrimitiveException {
    public UnknownClient() {
    }

    public UnknownClient(String message) {
      super(message);
    }
  }

  /**
   * Unknown session exception.
   */
  public static class UnknownSession extends PrimitiveException {
    public UnknownSession() {
    }

    public UnknownSession(String message) {
      super(message);
    }
  }

  /**
   * Unknown service exception.
   */
  public static class UnknownService extends PrimitiveException {
    public UnknownService() {
    }

    public UnknownService(String message) {
      super(message);
    }
  }

  /**
   * Closed session exception.
   */
  public static class ClosedSession extends PrimitiveException {
    public ClosedSession() {
    }

    public ClosedSession(String message) {
      super(message);
    }
  }
}
