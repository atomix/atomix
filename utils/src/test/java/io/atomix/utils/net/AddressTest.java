/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.utils.net;

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
    assertEquals("127.0.0.1:5000", address.toString());
  }

  @Test
  public void testIPv6Address() throws Exception {
    Address address = Address.from("[FE80:CD00:0000:0CDE:1257:0000:211E:729C]:5000");
    assertEquals("FE80:CD00:0000:0CDE:1257:0000:211E:729C", address.host());
    assertEquals(5000, address.port());
    assertEquals("[FE80:CD00:0000:0CDE:1257:0000:211E:729C]:5000", address.toString());
  }
}
