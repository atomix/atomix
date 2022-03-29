// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import io.atomix.utils.misc.TimestampPrinter;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Timestamp printer test.
 */
public class TimestampPrinterTest {
  @Test
  @Ignore // Timestamp is environment specific
  public void testTimestampPrinter() throws Exception {
    TimestampPrinter printer = TimestampPrinter.of(1);
    assertEquals("1969-12-31 04:00:00,001", printer.toString());
  }
}
