// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.logging;

import org.slf4j.LoggerFactory;

/**
 * Contextual logger factory.
 */
public class ContextualLoggerFactory {

  /**
   * Returns a contextual logger.
   *
   * @param name the contextual logger name
   * @param context the logger context
   * @return the logger
   */
  public static ContextualLogger getLogger(String name, LoggerContext context) {
    return new ContextualLogger(LoggerFactory.getLogger(name), context);
  }

  /**
   * Returns a contextual logger.
   *
   * @param clazz the contextual logger class
   * @param context the logger context
   * @return the logger
   */
  public static ContextualLogger getLogger(Class clazz, LoggerContext context) {
    return new ContextualLogger(LoggerFactory.getLogger(clazz), context);
  }

  private ContextualLoggerFactory() {
  }
}
