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
import org.testng.annotations.Test;

import java.io.Serializable;
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
   * Tests serializing a boolean.
   */
  public void testSerializeBoolean() {
    Serializer serializer = new Serializer();
    Buffer buffer = serializer.writeObject(true).flip();
    boolean result = serializer.readObject(buffer);
    assertTrue(result);
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
    Serializer serializer = new Serializer();
    Map<String, TestPojoWithWriter> map = new HashMap<>();
    TestPojoWithWriter value1 = new TestPojoWithWriter();
    value1.primitive = 10;
    value1.string = "Hello world!";
    map.put("foo", value1);
    TestPojoWithWriter value2 = new TestPojoWithWriter();
    value2.primitive = 100;
    TestWritableWithId writable = new TestWritableWithId();
    writable.primitive = 200;
    writable.string = "Hello world again!";
    value2.object = writable;
    map.put("bar", value2);
    TestPojoWithWriter value3 = new TestPojoWithWriter();
    value3.primitive = -100;
    map.put("baz", value3);
    Buffer buffer = serializer.writeObject(map).flip();
    Map<String, TestPojoWithWriter> result = serializer.readObject(buffer);
    assertEquals(result.get("foo").primitive, 10);
    assertNull(result.get("foo").object);
    assertEquals(result.get("foo").string, "Hello world!");
    assertEquals(result.get("bar").primitive, 100);
    assertEquals(((TestWritableWithId) result.get("bar").object).primitive, 200);
    assertNull(((TestWritableWithId) result.get("bar").object).object);
    assertEquals(((TestWritableWithId) result.get("bar").object).string, "Hello world again!");
    assertEquals(result.get("baz").primitive, -100);
    assertNull(result.get("baz").object);
    assertNull(result.get("baz").string);
  }

  /**
   * Tests serializing a writable with a configured ID.
   */
  public void testSerializeWritableWithId() {
    Serializer serializer = new Serializer();
    TestWritableWithId writable = new TestWritableWithId();
    writable.primitive = 100;
    TestPojoWithWriter pojo = new TestPojoWithWriter();
    pojo.primitive = 200;
    pojo.object = null;
    pojo.string = "Hello world again!";
    writable.object = pojo;
    writable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(writable).flip();
    TestWritableWithId result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestPojoWithWriter) result.object).primitive, 200);
    assertNull(((TestPojoWithWriter) result.object).object);
    assertEquals(((TestPojoWithWriter) result.object).string, "Hello world again!");
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing a writable without an ID.
   */
  public void testSerializeWritableWithoutId() {
    Serializer serializer = new Serializer();
    TestWritableWithoutId writable = new TestWritableWithoutId();
    writable.primitive = 100;
    TestPojoWithWriter pojo = new TestPojoWithWriter();
    pojo.primitive = 200;
    pojo.object = null;
    pojo.string = "Hello world again!";
    writable.object = pojo;
    writable.string = "Hello world!";
    Buffer buffer = serializer.writeObject(writable).flip();
    TestWritableWithoutId result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestPojoWithWriter) result.object).primitive, 200);
    assertNull(((TestPojoWithWriter) result.object).object);
    assertEquals(((TestPojoWithWriter) result.object).string, "Hello world again!");
    assertEquals(result.string, "Hello world!");
  }

  /**
   * Tests serializing a POJO with an object writer.
   */
  public void testSerializeWriter() {
    Serializer serializer = new Serializer();
    TestPojoWithWriter pojo = new TestPojoWithWriter();
    pojo.primitive = 100;
    TestWritableWithId writable = new TestWritableWithId();
    writable.primitive = 200;
    writable.object = null;
    writable.string = "Hello world again!";
    pojo.object = writable;
    pojo.string = "Hello world!";
    Buffer buffer = serializer.writeObject(pojo).flip();
    TestPojoWithWriter result = serializer.readObject(buffer);
    assertEquals(result.primitive, 100);
    assertEquals(((TestWritableWithId) result.object).primitive, 200);
    assertNull(((TestWritableWithId) result.object).object);
    assertEquals(((TestWritableWithId) result.object).string, "Hello world again!");
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

  public static abstract class TestWritable implements Writable {
    protected long primitive;
    protected Object object;
    protected String string;

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      buffer.writeLong(primitive);
      serializer.writeObject(object, buffer);
      buffer.writeUTF8(string);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      primitive = buffer.readLong();
      object = serializer.readObject(buffer);
      string = buffer.readUTF8();
    }
  }

  @SerializeWith(id=100)
  public static class TestWritableWithId extends TestWritable {
  }

  public static class TestWritableWithoutId extends TestWritable {
  }

  public static class TestPojoWithWriter {
    protected long primitive;
    protected Object object;
    protected String string;
  }

  @Serialize(@Serialize.Type(id=101, type=TestPojoWithWriter.class))
  public static class TestWriter implements ObjectWriter<TestPojoWithWriter> {
    @Override
    public void write(TestPojoWithWriter object, Buffer buffer, Serializer serializer) {
      buffer.writeLong(object.primitive);
      serializer.writeObject(object.object, buffer);
      buffer.writeUTF8(object.string);
    }

    @Override
    public TestPojoWithWriter read(Class<TestPojoWithWriter> type, Buffer buffer, Serializer serializer) {
      TestPojoWithWriter object = new TestPojoWithWriter();
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

}
