// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KryoOutputPoolTest {

  private KryoOutputPool kryoOutputPool;

  @Before
  public void setUp() throws Exception {
    kryoOutputPool = new KryoOutputPool();
  }

  @Test
  public void discardOutput() {
    final Output[] result = new Output[2];
    kryoOutputPool.run(output -> {
      result[0] = output;
      return null;
    }, KryoOutputPool.MAX_POOLED_BUFFER_SIZE + 1);
    kryoOutputPool.run(output -> {
      result[1] = output;
      return null;
    }, 0);
    assertTrue(result[0] != result[1]);
  }

  @Test
  public void recycleOutput() {
    final ByteArrayOutput[] result = new ByteArrayOutput[2];
    kryoOutputPool.run(output -> {
      output.writeInt(1);
      assertEquals(Integer.BYTES, output.position());
      result[0] = output;
      return null;
    }, 0);
    assertEquals(0, result[0].position());
    assertEquals(0, result[0].getByteArrayOutputStream().size());
    kryoOutputPool.run(output -> {
      assertEquals(0, output.position());
      result[1] = output;
      return null;
    }, 0);
    assertTrue(result[0] == result[1]);
  }
}
