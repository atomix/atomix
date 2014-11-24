package net.kuujo.copycat.log;

import java.nio.ByteBuffer;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Tests entry implementations.
 */
@Test
@SuppressWarnings("unchecked")
public class EntryTest {
  /**
   * Returns a set of entry types/impls to test.
   */
  @DataProvider
  private Object[][] entryProvider() {
    return new Object[][] {
        { OperationEntry.class, new OperationEntry(1, "foo", "bar", "baz") },
        { ConfigurationEntry.class, new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")) },
        { SnapshotEntry.class,
            new SnapshotEntry(1, new Cluster("foo", "bar", "baz"), new byte[] { 1, 2, 3 }) } };
  }

  @Test(dataProvider = "entryProvider")
  @SuppressWarnings({ "rawtypes" })
  public <T extends Entry> void testSerializeAndDeserializeEntry(Class<T> entryType, T entry)
    throws Exception {
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
