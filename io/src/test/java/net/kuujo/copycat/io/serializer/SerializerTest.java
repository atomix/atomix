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
package net.kuujo.copycat.io.serializer;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

import static org.testng.Assert.*;

/**
 * Serializer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class SerializerTest {

  /**
   * Tests serializing a string.
   */
  public void testSerializeString() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject("Hello world!").flip();
    String result = serializer.readObject(buffer);
    assertEquals(result, "Hello world!");
  }

  /**
   * Tests serializing a byte.
   */
  public void testSerializeByte() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject((byte) 123).flip();
    byte result = serializer.readObject(buffer);
    assertEquals(result, 123);
  }

  /**
   * Tests serializing a boolean.
   */
  public void testSerializeBoolean() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(true).flip();
    boolean result = serializer.readObject(buffer);
    assertTrue(result);
  }

  /**
   * Tests serializing a character.
   */
  public void testSerializeCharacter() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject((char) 123).flip();
    char result = serializer.readObject(buffer);
    assertEquals(result, (char) 123);
  }

  /**
   * Tests serializing a float.
   */
  public void testSerializeFloat() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(1.234f).flip();
    float result = serializer.readObject(buffer);
    assertEquals(result, 1.234f);
  }

  /**
   * Tests serializing a double.
   */
  public void testSerializeDouble() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(1.234d).flip();
    double result = serializer.readObject(buffer);
    assertEquals(result, 1.234d);
  }

  /**
   * Tests serializing a short.
   */
  public void testSerializeShort() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject((short) 1234).flip();
    short result = serializer.readObject(buffer);
    assertEquals(result, (short) 1234);
  }

  /**
   * Tests serializing a integer.
   */
  public void testSerializeInteger() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(1234).flip();
    int result = serializer.readObject(buffer);
    assertEquals(result, 1234);
  }

  /**
   * Tests serializing a long.
   */
  public void testSerializeLong() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(1234l).flip();
    long result = serializer.readObject(buffer);
    assertEquals(result, 1234l);
  }

  /**
   * Tests serializing a byte array.
   */
  public void testSerializeByteArray() {
    Serializer serializer = new Serializer();
    byte[] bytes = new byte[]{100, 101, 102, 103, 104};
    Buffer buffer = serializer.writeObject(bytes).flip();
    byte[] result = serializer.readObject(buffer);
    assertEquals(100, result[0]);
    assertEquals(101, result[1]);
    assertEquals(102, result[2]);
    assertEquals(103, result[3]);
    assertEquals(104, result[4]);
  }

  /**
   * Tests serializing a boolean array.
   */
  public void testSerializeBooleanArray() {
    Serializer serializer = new Serializer();
    boolean[] booleans = new boolean[]{true, false, true, true, false};
    Buffer buffer = serializer.writeObject(booleans).flip();
    boolean[] result = serializer.readObject(buffer);
    assertTrue(result[0]);
    assertFalse(result[1]);
    assertTrue(result[2]);
    assertTrue(result[3]);
    assertFalse(result[4]);
  }

  /**
   * Tests serializing a character array.
   */
  public void testSerializeCharacterArray() {
    Serializer serializer = new Serializer();
    char[] chars = new char[]{100, 101, 102, 103, 104};
    Buffer buffer = serializer.writeObject(chars).flip();
    char[] result = serializer.readObject(buffer);
    assertEquals(100, result[0]);
    assertEquals(101, result[1]);
    assertEquals(102, result[2]);
    assertEquals(103, result[3]);
    assertEquals(104, result[4]);
  }

  /**
   * Tests serializing a short array.
   */
  public void testSerializeShortArray() {
    Serializer serializer = new Serializer();
    short[] shorts = new short[]{100, 101, 102, 103, 104};
    Buffer buffer = serializer.writeObject(shorts).flip();
    short[] result = serializer.readObject(buffer);
    assertEquals(100, result[0]);
    assertEquals(101, result[1]);
    assertEquals(102, result[2]);
    assertEquals(103, result[3]);
    assertEquals(104, result[4]);
  }

  /**
   * Tests serializing an integer array.
   */
  public void testSerializeIntegerArray() {
    Serializer serializer = new Serializer();
    int[] ints = new int[]{100, 101, 102, 103, 104};
    Buffer buffer = serializer.writeObject(ints).flip();
    int[] result = serializer.readObject(buffer);
    assertEquals(100, result[0]);
    assertEquals(101, result[1]);
    assertEquals(102, result[2]);
    assertEquals(103, result[3]);
    assertEquals(104, result[4]);
  }

  /**
   * Tests serializing a long array.
   */
  public void testSerializeLongArray() {
    Serializer serializer = new Serializer();
    long[] longs = new long[]{100, 101, 102, 103, 104};
    Buffer buffer = serializer.writeObject(longs).flip();
    long[] result = serializer.readObject(buffer);
    assertEquals(100, result[0]);
    assertEquals(101, result[1]);
    assertEquals(102, result[2]);
    assertEquals(103, result[3]);
    assertEquals(104, result[4]);
  }

  /**
   * Tests serializing a float array.
   */
  public void testSerializeFloatArray() {
    Serializer serializer = new Serializer();
    float[] floats = new float[]{100.1f, 101.2f, 102.3f, 103.4f, 104.5f};
    Buffer buffer = serializer.writeObject(floats).flip();
    float[] result = serializer.readObject(buffer);
    assertEquals(100.1f, result[0]);
    assertEquals(101.2f, result[1]);
    assertEquals(102.3f, result[2]);
    assertEquals(103.4f, result[3]);
    assertEquals(104.5f, result[4]);
  }

  /**
   * Tests serializing a double array.
   */
  public void testSerializeDoubleArray() {
    Serializer serializer = new Serializer();
    double[] doubles = new double[]{100.1d, 101.2d, 102.3d, 103.4d, 104.5d};
    Buffer buffer = serializer.writeObject(doubles).flip();
    double[] result = serializer.readObject(buffer);
    assertEquals(100.1d, result[0]);
    assertEquals(101.2d, result[1]);
    assertEquals(102.3d, result[2]);
    assertEquals(103.4d, result[3]);
    assertEquals(104.5d, result[4]);
  }

  /**
   * Tests serializing a time zone.
   */
  public void testSerializeTimeZone() {
    Serializer serializer = new Serializer();
    TimeZone timeZone = TimeZone.getDefault();
    Buffer buffer = serializer.writeObject(timeZone).flip();
    TimeZone result = serializer.readObject(buffer);
    assertEquals(timeZone, result);
  }

  /**
   * Tests serializing a date.
   */
  public void testSerializeDate() {
    Serializer serializer = new Serializer();
    Date date = new Date(System.currentTimeMillis());
    Buffer buffer = serializer.writeObject(date).flip();
    Date result = serializer.readObject(buffer);
    assertEquals(date, result);
  }

  /**
   * Tests serializing a calendar.
   */
  public void testSerializeCalendar() {
    Serializer serializer = new Serializer();
    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.setTimeInMillis(System.currentTimeMillis());
    Buffer buffer = serializer.writeObject(calendar).flip();
    Calendar result = serializer.readObject(buffer);
    assertEquals(calendar, result);
  }

  /**
   * Tests serializing a class.
   */
  public void testSerializeClass() {
    Serializer serializer = new Serializer();
    Class clazz = SerializerTest.class;
    Buffer buffer = serializer.writeObject(clazz).flip();
    Class result = serializer.readObject(buffer);
    assertEquals(clazz, result);
  }

  /**
   * Tests serializing an enum.
   */
  public void testSerializeEnum() {
    Serializer serializer = new Serializer();
    TestEnum test = TestEnum.THREE;
    Buffer buffer = serializer.writeObject(test).flip();
    Enum result = serializer.readObject(buffer);
    assertEquals(test, result);
  }

  /**
   * Tests serializing a list.
   */
  public void testSerializeList() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(Arrays.asList(1, 2, 3)).flip();
    List result = serializer.readObject(buffer);
    assertEquals(result, Arrays.asList(1, 2, 3));
  }

  /**
   * Tests serializing a set.
   */
  public void testSerializeSet() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(new HashSet<>(Arrays.asList(1, 2, 3))).flip();
    Set result = serializer.readObject(buffer);
    assertEquals(result, new HashSet<>(Arrays.asList(1, 2, 3)));
  }

  /**
   * Tests serializing a map.
   */
  public void testSerializeMap() {
    Serializer serializer = new Serializer()
      .register(TestCopycatSerializable.class, 100)
      .register(TestPojoWithSerializer.class, TestSerializer.class);
    Map<String, TestPojoWithSerializer> map = new HashMap<>();
    TestPojoWithSerializer value1 = new TestPojoWithSerializer();
    value1.primitive = 10;
    value1.string = "Hello world!";
    map.put("foo", value1);
    TestPojoWithSerializer value2 = new TestPojoWithSerializer();
    value2.primitive = 100;
    TestCopycatSerializable serializable = new TestCopycatSerializable();
    serializable.primitive = 200;
    serializable.string = "Hello world again!";
    value2.object = serializable;
    map.put("bar", value2);
    TestPojoWithSerializer value3 = new TestPojoWithSerializer();
    value3.primitive = -100;
    map.put("baz", value3);
    Buffer buffer = serializer.writeObject(map).flip();
    Map<String, TestPojoWithSerializer> result = serializer.readObject(buffer);
    assertEquals(result.get("foo").primitive, 10);
    assertNull(result.get("foo").object);
    assertEquals(result.get("foo").string, "Hello world!");
    assertEquals(result.get("bar").primitive, 100);
    assertEquals(((TestCopycatSerializable) result.get("bar").object).primitive, 200);
    assertNull(((TestCopycatSerializable) result.get("bar").object).object);
    assertEquals(((TestCopycatSerializable) result.get("bar").object).string, "Hello world again!");
    assertEquals(result.get("baz").primitive, -100);
    assertNull(result.get("baz").object);
    assertNull(result.get("baz").string);
  }

  /**
   * Tests copying a map.
   */
  public void testCopyMap() {
    Serializer serializer = new Serializer();
    Map<String, String> map = new HashMap<>();
    map.put("foo", "Hello world!");
    map.put("bar", "Hello world again!");
    Map<String, String> result = serializer.copy(map);
    assertEquals(map, result);
    assertEquals(map.get("foo"), "Hello world!");
    assertEquals(map.get("bar"), "Hello world again!");
  }

  /**
   * Tests serializing a writable with a configured ID.
   */
  public void testSerializeWritableWithId() {
    Serializer serializer = new Serializer()
      .register(TestCopycatSerializable.class, 100)
      .register(TestPojoWithSerializer.class, TestSerializer.class);
    TestCopycatSerializable writable = new TestCopycatSerializable();
    writable.primitive = 100;
    TestPojoWithSerializer pojo = new TestPojoWithSerializer();
    pojo.primitive = 200;
    pojo.object = null;
    pojo.string = "Hello world again!";
    writable.object = pojo;
    writable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(writable).flip();
    TestCopycatSerializable result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestPojoWithSerializer) result.object).primitive, 200);
    assertNull(((TestPojoWithSerializer) result.object).object);
    assertEquals(((TestPojoWithSerializer) result.object).string, "Hello world again!");
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing a writable without an ID.
   */
  public void testSerializeWritableWithoutId() {
    Serializer serializer = new Serializer()
      .register(TestCopycatSerializable.class, 100)
      .register(TestPojoWithSerializer.class, TestSerializer.class);
    TestCopycatSerializable writable = new TestCopycatSerializable();
    writable.primitive = 100;
    TestPojoWithSerializer pojo = new TestPojoWithSerializer();
    pojo.primitive = 200;
    pojo.object = null;
    pojo.string = "Hello world again!";
    writable.object = pojo;
    writable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(writable).flip();
    TestCopycatSerializable result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestPojoWithSerializer) result.object).primitive, 200);
    assertNull(((TestPojoWithSerializer) result.object).object);
    assertEquals(((TestPojoWithSerializer) result.object).string, "Hello world again!");
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing a POJO with a serializer.
   */
  public void testSerializeSerializer() {
    Serializer serializer = new Serializer()
      .register(TestCopycatSerializable.class, 100)
      .register(TestPojoWithSerializer.class, TestSerializer.class);
    TestPojoWithSerializer pojo = new TestPojoWithSerializer();
    pojo.primitive = 100;
    TestCopycatSerializable writable = new TestCopycatSerializable();
    writable.primitive = 200;
    writable.object = null;
    writable.string = "Hello world again!";
    pojo.object = writable;
    pojo.string = "Hello world!";
    Buffer buffer = serializer.writeObject(pojo).flip();
    TestPojoWithSerializer result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestCopycatSerializable) result.object).primitive, 200);
    assertNull(((TestCopycatSerializable) result.object).object);
    assertEquals(((TestCopycatSerializable) result.object).string, "Hello world again!");
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing a serializable.
   */
  public void testSerializeSerializable() {
    Serializer serializer = new Serializer();
    TestSerializable serializable = new TestSerializable();
    serializable.primitive = 100;
    serializable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(serializable).flip();
    TestSerializable result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing an externalizable.
   */
  public void testSerializeExternalizable() {
    Serializer serializer = new Serializer();
    serializer.register(TestExternalizable.class);
    TestExternalizable externalizable = new TestExternalizable();
    externalizable.primitive = 100;
    externalizable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(externalizable).flip();
    TestExternalizable result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing classes registered via ServiceLoaderTypeResolver during construction.
   */
  public void testResolverConstructor() {
    testServiceLoaderResolver(new Serializer(new ServiceLoaderTypeResolver()));
  }

  /**
   * Tests serializing classes registered via ServiceLoaderTypeResolver after construction.
   */
  public void testResolverMethod() {
    testServiceLoaderResolver(new Serializer().resolve(new ServiceLoaderTypeResolver()));
  }

  /**
   * Tests serializing classes registered via ServiceLoaderTypeResolver multiple times.
   */
  public void testResolverConstructorAndMethod() {
    testServiceLoaderResolver(new Serializer(new ServiceLoaderTypeResolver()).resolve(new ServiceLoaderTypeResolver()));
  }

  /**
   * Tests serializing classes registered via ServiceLoaderTypeResolver.
   */
  private void testServiceLoaderResolver(Serializer serializer) {
    TestSerializeWithId withId = new TestSerializeWithId();
    withId.primitive = 1;
    TestSerializeWithoutId withoutId = new TestSerializeWithoutId();
    withoutId.primitive = 2;
    Buffer buffer = serializer.writeObject(withoutId, serializer.writeObject(withId)).flip();
    assertEquals(serializer.<TestSerializeWithId>readObject(buffer).primitive, 1);
    assertEquals(serializer.<TestSerializeWithoutId>readObject(buffer).primitive, 2);
  }

  public static class TestCopycatSerializable implements CopycatSerializable {
    protected long primitive;
    protected Object object;
    protected String string;

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeLong(primitive);
      serializer.writeObject(object, buffer);
      buffer.writeUTF8(string);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      primitive = buffer.readLong();
      object = serializer.readObject(buffer);
      string = buffer.readUTF8();
    }
  }

  public static class TestPojoWithSerializer {
    protected long primitive;
    protected Object object;
    protected String string;
  }

  public static class TestSerializer implements TypeSerializer<TestPojoWithSerializer> {
    @Override
    public void write(TestPojoWithSerializer object, BufferOutput buffer, Serializer serializer) {
      buffer.writeLong(object.primitive);
      serializer.writeObject(object.object, buffer);
      buffer.writeUTF8(object.string);
    }

    @Override
    public TestPojoWithSerializer read(Class<TestPojoWithSerializer> type, BufferInput buffer, Serializer serializer) {
      TestPojoWithSerializer object = new TestPojoWithSerializer();
      object.primitive = buffer.readLong();
      object.object = serializer.readObject(buffer);
      object.string = buffer.readUTF8();
      return object;
    }
  }

  public static class TestSerializable implements Serializable {
    protected long primitive;
    protected String string;
  }

  public static class TestExternalizable implements Externalizable {
    protected long primitive;
    protected String string;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeLong(primitive);
      out.writeUTF(string);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      primitive = in.readLong();
      string = in.readUTF();
    }
  }

  @SerializeWith(id=0)
  public static class TestSerializeWithId implements CopycatSerializable {
    protected byte primitive;

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeByte(primitive);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      primitive = (byte) buffer.readByte();
    }
  }

  @SerializeWith
  public static class TestSerializeWithoutId implements CopycatSerializable {
    protected byte primitive;

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeByte(primitive);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      primitive = (byte) buffer.readByte();
    }
  }

  public static enum TestEnum {
    ONE,
    TWO,
    THREE
  }

}
