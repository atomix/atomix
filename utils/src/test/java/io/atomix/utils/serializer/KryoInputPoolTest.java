// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.io.Input;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KryoInputPoolTest {

  private KryoInputPool kryoInputPool;

  @Before
  public void setUp() throws Exception {
    kryoInputPool = new KryoInputPool();
  }

  @Test
  public void discardOutput() {
    final Input[] result = new Input[2];
    kryoInputPool.run(input -> {
      result[0] = input;
      return null;
    }, KryoInputPool.MAX_POOLED_BUFFER_SIZE + 1);
    kryoInputPool.run(input -> {
      result[1] = input;
      return null;
    }, 0);
    assertTrue(result[0] != result[1]);
  }

  @Test
  public void recycleOutput() {
    final Input[] result = new Input[2];
    kryoInputPool.run(input -> {
      assertEquals(0, input.position());
      byte[] payload = new byte[]{1, 2, 3, 4};
      input.setBuffer(payload);
      assertArrayEquals(payload, input.readBytes(4));
      result[0] = input;
      return null;
    }, 0);
    assertNull(result[0].getInputStream());
    assertEquals(0, result[0].position());
    kryoInputPool.run(input -> {
      result[1] = input;
      return null;
    }, 0);
    assertTrue(result[0] == result[1]);
  }
}
