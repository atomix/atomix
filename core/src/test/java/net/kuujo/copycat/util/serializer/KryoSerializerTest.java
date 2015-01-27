/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util.serializer;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.internal.MemberInfo;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Kryo serializer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class KryoSerializerTest {

  /**
   * Tests serializing a collection of member info.
   */
  public void testSerializeMemberInfo() {
    Serializer serializer = new KryoSerializer();
    Map<String, MemberInfo> info = new HashMap<>();
    info.put("local://test1", new MemberInfo("local://test1", Member.Type.ACTIVE, Member.State.ALIVE));
    info.put("local://test2", new MemberInfo("local://test2", Member.Type.ACTIVE, Member.State.ALIVE));
    info.put("local://test3", new MemberInfo("local://test3", Member.Type.PASSIVE, Member.State.SUSPICIOUS));
    ByteBuffer buffer = serializer.writeObject(new ArrayList<>(info.values()));
    List<MemberInfo> result = serializer.readObject(buffer);
    assertEquals(result.get(0).uri(), "local://test1");
    assertTrue(result.get(0).type() == Member.Type.ACTIVE);
    assertTrue(result.get(0).state() == Member.State.ALIVE);
    assertEquals(result.get(1).uri(), "local://test2");
    assertTrue(result.get(1).type() == Member.Type.ACTIVE);
    assertTrue(result.get(1).state() == Member.State.ALIVE);
    assertEquals(result.get(2).uri(), "local://test3");
    assertTrue(result.get(2).type() == Member.Type.PASSIVE);
    assertTrue(result.get(2).state() == Member.State.SUSPICIOUS);
  }

}
