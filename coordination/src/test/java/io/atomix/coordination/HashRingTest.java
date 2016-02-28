/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.coordination;

import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.coordination.state.HashRing;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Hash ring test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class HashRingTest {

  /**
   * Tests getting a list of members from a hash ring.
   */
  public void testHashRingMembers() throws Throwable{
    HashRing hashRing = new HashRing(new Murmur2Hasher(), 100, 3);
    hashRing.addMember("1");
    hashRing.addMember("2");
    hashRing.addMember("3");
    hashRing.addMember("4");
    hashRing.addMember("5");
    List<String> members = hashRing.members("12345".getBytes());
    assertEquals(members.size(), 3);
  }

  /**
   * Tests getting members from a hash ring with too few replicas.
   */
  public void testHashRingFewerReplicas() throws Throwable {
    HashRing hashRing = new HashRing(new Murmur2Hasher(), 100, 3);
    hashRing.addMember("1");
    hashRing.addMember("2");
    List<String> members = hashRing.members("12345".getBytes());
    assertEquals(members.size(), 3);
  }

}
