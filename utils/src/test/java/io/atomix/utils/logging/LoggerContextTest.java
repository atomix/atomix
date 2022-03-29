// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.logging;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Contextual logger test.
 */
public class LoggerContextTest {
  @Test
  public void testLoggerContext() throws Exception {
    LoggerContext context = LoggerContext.builder("test")
        .addValue(1)
        .add("foo", "bar")
        .build();
    assertEquals("test{1}{foo=bar}", context.toString());
  }
}
