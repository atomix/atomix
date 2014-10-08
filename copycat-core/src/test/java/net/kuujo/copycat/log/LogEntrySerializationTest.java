package net.kuujo.copycat.log;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.log.CommandEntry;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.NoOpEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@Test
public class LogEntrySerializationTest {
  public void testSerializeNoOpEntry() throws Exception {
    testSerializeEntry(NoOpEntry.class, new NoOpEntry(1));
  }

  public void testDeserializeNoOpEntry() throws Exception {
    testDeserializeEntry(NoOpEntry.class, new NoOpEntry(1));
  }

  public void testSerializeCommandEntry() throws Exception {
    testSerializeEntry(CommandEntry.class, new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
  }

  public void testDeserializeCommandEntry() throws Exception {
    testDeserializeEntry(CommandEntry.class,
        new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
  }

  public void testSerializeConfigurationEntry() throws Exception {
    testSerializeEntry(ConfigurationEntry.class,
        new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
            .withRemoteMembers(new Member("bar"), new Member("baz"))));
  }

  public void testDeserializeConfigurationEntry() throws Exception {
    testDeserializeEntry(ConfigurationEntry.class,
        new ConfigurationEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
            .withRemoteMembers(new Member("bar"), new Member("baz"))));
  }

  public void testSerializeSnapshotEntry() throws Exception {
    testSerializeEntry(SnapshotEntry.class,
        new SnapshotEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
            .withRemoteMembers(new Member("bar"), new Member("baz")), new byte[] {1, 2, 3}));
  }

  public void testDeserializeSnapshotEntry() throws Exception {
    testSerializeEntry(SnapshotEntry.class,
        new SnapshotEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
            .withRemoteMembers(new Member("bar"), new Member("baz")), new byte[] {1, 2, 3}));
  }

  @SuppressWarnings("rawtypes")
  private <T extends Entry> void testSerializeEntry(Class<T> entryType, T entry) throws Exception {
    Kryo kryo = new Kryo();
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    Output output = new ByteBufferOutput(buffer);
    Class<? extends Serializer> serializer = entryType.getAnnotation(EntryType.class).serializer();
    kryo.register(entryType, serializer.newInstance(), entryType.getAnnotation(EntryType.class)
        .id());
    kryo.writeClassAndObject(output, entry);
  }

  @SuppressWarnings({"rawtypes"})
  private <T extends Entry> void testDeserializeEntry(Class<T> entryType, T entry) throws Exception {
    Kryo kryo = new Kryo();
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    Output output = new ByteBufferOutput(buffer);
    Input input = new ByteBufferInput(buffer);
    Class<? extends Serializer> serializer = entryType.getAnnotation(EntryType.class).serializer();
    kryo.register(entryType, serializer.newInstance(), entryType.getAnnotation(EntryType.class)
        .id());
    kryo.writeClassAndObject(output, entry);
    T result = (T) kryo.readClassAndObject(input);
    Assert.assertEquals(entry, result);
  }
}
