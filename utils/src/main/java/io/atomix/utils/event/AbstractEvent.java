// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base event implementation.
 */
public class AbstractEvent<T extends Enum, S> implements Event<T, S> {
  private final long time;
  private final T type;
  private final S subject;

  /**
   * Creates an event of a given type and for the specified subject and the
   * current time.
   *
   * @param type    event type
   * @param subject event subject
   */
  protected AbstractEvent(T type, S subject) {
    this(type, subject, System.currentTimeMillis());
  }

  /**
   * Creates an event of a given type and for the specified subject and time.
   *
   * @param type    event type
   * @param subject event subject
   * @param time    occurrence time
   */
  protected AbstractEvent(T type, S subject, long time) {
    this.type = type;
    this.subject = subject;
    this.time = time;
  }

  @Override
  public long time() {
    return time;
  }

  @Override
  public T type() {
    return type;
  }

  @Override
  public S subject() {
    return subject;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("time", new TimestampPrinter(time))
        .add("type", type())
        .add("subject", subject())
        .toString();
  }
}
