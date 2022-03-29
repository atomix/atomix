// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import io.atomix.utils.misc.ArraySizeHashPrinter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Array size hash printer test.
 */
public class ArraySizeHashPrinterTest {
  @Test
  public void testArraySizeHashPrinter() throws Exception {
    ArraySizeHashPrinter printer = ArraySizeHashPrinter.of(new byte[]{1, 2, 3});
    assertEquals("byte[]{length=3, hash=30817}", printer.toString());
  }
}
