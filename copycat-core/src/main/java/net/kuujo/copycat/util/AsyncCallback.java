package net.kuujo.copycat.util;

/**
 * An asynchronous handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncCallback<T> {

  /**
   * Called when the asynchronous action is complete.
   *
   * @param value The callback argument.
   */
  void complete(T value);

  /**
   * Fails the asynchronous action.
   *
   * @param t The failure cause.
   */
  void fail(Throwable t);

}
