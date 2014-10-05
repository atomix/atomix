/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.log.Compactable;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.internal.CommandEntry;
import net.kuujo.copycat.log.internal.ConfigurationEntry;
import net.kuujo.copycat.log.internal.NoOpEntry;
import net.kuujo.copycat.log.internal.SnapshotEntry;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * Base log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("unchecked")
public class LogTest {

  /**
   * Creates a test log instance.
   */
  protected Log createLog() {
    return null;
  }

  @Test
  public void testSerializeNoOpEntry() throws Exception {
    testSerializeEntry(NoOpEntry.class, new NoOpEntry(1));
  }

  @Test
  public void testDeserializeNoOpEntry() throws Exception {
    testDeserializeEntry(NoOpEntry.class, new NoOpEntry(1));
  }

  @Test
  public void testSerializeCommandEntry() throws Exception {
    testSerializeEntry(CommandEntry.class, new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
  }

  @Test
  public void testDeserializeCommandEntry() throws Exception {
    testDeserializeEntry(CommandEntry.class, new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
  }

  @Test
  public void testSerializeConfigurationEntry() throws Exception {
    testSerializeEntry(ConfigurationEntry.class, new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
  }

  @Test
  public void testDeserializeConfigurationEntry() throws Exception {
    testDeserializeEntry(ConfigurationEntry.class, new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
  }

  @Test
  public void testSerializeSnapshotEntry() throws Exception {
    testSerializeEntry(SnapshotEntry.class, new SnapshotEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz")), new byte[]{1, 2, 3}));
  }

  @Test
  public void testDeserializeSnapshotEntry() throws Exception {
    testSerializeEntry(SnapshotEntry.class, new SnapshotEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz")), new byte[]{1, 2, 3}));
  }

  @SuppressWarnings("rawtypes")
  private <T extends Entry> void testSerializeEntry(Class<T> entryType, T entry) throws Exception {
    Kryo kryo = new Kryo();
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    Output output = new ByteBufferOutput(buffer);
    Class<? extends Serializer> serializer = entryType.getAnnotation(EntryType.class).serializer();
    kryo.register(entryType, serializer.newInstance(), entryType.getAnnotation(EntryType.class).id());
    kryo.writeClassAndObject(output, entry);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T extends Entry> void testDeserializeEntry(Class<T> entryType, T entry) throws Exception {
    Kryo kryo = new Kryo();
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    Output output = new ByteBufferOutput(buffer);
    Input input = new ByteBufferInput(buffer);
    Class<? extends Serializer> serializer = entryType.getAnnotation(EntryType.class).serializer();
    kryo.register(entryType, serializer.newInstance(), entryType.getAnnotation(EntryType.class).id());
    kryo.writeClassAndObject(output, entry);
    T result = (T) kryo.readClassAndObject(input);
    Assert.assertEquals(entry, result);
  }

  @Test
  public void testAppendEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    log.close();
  }

  @Test
  public void testContainsEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    Assert.assertTrue(log.containsEntry(1));
    log.close();
  }

  @Test
  public void testGetEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Entry entry = log.getEntry(index);
    Assert.assertTrue(entry instanceof NoOpEntry);
    log.close();
  }

  @Test
  public void testFirstIndex() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    Assert.assertTrue(log.firstIndex() == 1);
    Assert.assertTrue(log.lastIndex() == 3);
    log.close();
  }

  @Test
  public void testFirstEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    Entry entry = log.firstEntry();
    Assert.assertTrue(entry instanceof NoOpEntry);
    log.close();
  }

  @Test
  public void testLastIndex() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    Assert.assertTrue(log.lastIndex() == 3);
    log.close();
  }

  @Test
  public void testLastEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    Entry entry = log.lastEntry();
    Assert.assertTrue(entry instanceof CommandEntry);
    log.close();
  }

  @Test
  public void testRemoveAfter() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 6);

    log.removeAfter(2);
    Assert.assertTrue(log.firstIndex() == 1);
    Assert.assertTrue(log.firstEntry() instanceof NoOpEntry);
    Assert.assertTrue(log.lastIndex() == 2);
    Assert.assertTrue(log.lastEntry() instanceof ConfigurationEntry);
    log.close();
  }

  @Test
  public void testCompactMiddleOfLog() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "bar", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "baz", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 5);
    if (log instanceof Compactable) {
      ((Compactable) log).compact(3, new SnapshotEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz")), "Hello world!".getBytes()));
      Assert.assertTrue(log.size() == 3);
      Assert.assertTrue(log.firstIndex() == 3);
      Assert.assertTrue(log.lastIndex() == 5);
      SnapshotEntry entry = log.getEntry(3);
      Assert.assertTrue(entry.term() == 1);
      Assert.assertEquals("Hello world!", new String(entry.data()));
      CommandEntry entry2 = log.getEntry(4);
      Assert.assertTrue(entry2.term() == 1);
      Assert.assertEquals("bar", entry2.command());
      CommandEntry entry3 = log.getEntry(5);
      Assert.assertTrue(entry3.term() == 1);
      Assert.assertEquals("baz", entry3.command());
    }
    log.close();
  }

  @Test
  public void testCompactEndOfLog() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 5);
    if (log instanceof Compactable) {
      ((Compactable) log).compact(5, new SnapshotEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz")), "Hello world!".getBytes()));
      Assert.assertTrue(log.size() == 1);
      Assert.assertTrue(log.firstIndex() == 5);
      Assert.assertTrue(log.lastIndex() == 5);
      SnapshotEntry entry = log.getEntry(5);
      Assert.assertTrue(entry.term() == 1);
      Assert.assertEquals("Hello world!", new String(entry.data()));
    }
    log.close();
  }

  @Test
  public void testManyOperations() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new MemberConfig("foo")).withRemoteMembers(new MemberConfig("bar"), new MemberConfig("baz"))));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    Assert.assertTrue(index == 6);
    log.close();
  }

}
