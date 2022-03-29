// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.net;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Address test.
 */
public class AddressTest {
  @Test
  public void testIPv4Address() throws Exception {
    Address address = Address.from("127.0.0.1:5000");
    assertEquals("127.0.0.1", address.host());
    assertEquals(5000, address.port());
    assertEquals("localhost", address.address().getHostName());
    assertEquals("127.0.0.1:5000", address.toString());
  }

  @Test
  public void testIPv6Address() throws Exception {
    Address address = Address.from("[fe80:cd00:0000:0cde:1257:0000:211e:729c]:5000");
    assertEquals("fe80:cd00:0000:0cde:1257:0000:211e:729c", address.host());
    assertEquals(5000, address.port());
    assertEquals("fe80:cd00:0:cde:1257:0:211e:729c", address.address().getHostName());
    assertEquals("[fe80:cd00:0000:0cde:1257:0000:211e:729c]:5000", address.toString());
  }

  @Test
  @Ignore
  public void testResolveAddress() throws Exception {
    Address address = Address.from("localhost", 5000);
    assertEquals("127.0.0.1", address.address().getHostAddress());
    assertEquals(5000, address.port());
  }
}
