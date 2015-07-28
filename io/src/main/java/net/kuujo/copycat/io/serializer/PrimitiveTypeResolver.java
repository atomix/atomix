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

import net.kuujo.copycat.io.serializer.lang.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Primitive type resolver.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PrimitiveTypeResolver implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final HashMap<Class<?>, Class<? extends TypeSerializer<?>>> SERIALIZERS = new LinkedHashMap() {{
    put(byte.class, ByteSerializer.class);
    put(Byte.class, ByteSerializer.class);
    put(byte[].class, ByteArraySerializer.class);
    put(Byte[].class, ByteArraySerializer.class);
    put(boolean.class, BooleanSerializer.class);
    put(Boolean.class, BooleanSerializer.class);
    put(boolean[].class, BooleanArraySerializer.class);
    put(Boolean[].class, BooleanArraySerializer.class);
    put(char.class, CharacterSerializer.class);
    put(Character.class, CharacterSerializer.class);
    put(char[].class, CharacterArraySerializer.class);
    put(Character[].class, CharacterArraySerializer.class);
    put(short.class, ShortSerializer.class);
    put(Short.class, ShortSerializer.class);
    put(short[].class, ShortArraySerializer.class);
    put(Short[].class, ShortArraySerializer.class);
    put(int.class, IntegerSerializer.class);
    put(Integer.class, IntegerSerializer.class);
    put(int[].class, IntegerArraySerializer.class);
    put(Integer[].class, IntegerArraySerializer.class);
    put(long.class, LongSerializer.class);
    put(Long.class, LongSerializer.class);
    put(long[].class, LongArraySerializer.class);
    put(Long[].class, LongArraySerializer.class);
    put(float.class, FloatSerializer.class);
    put(Float.class, FloatSerializer.class);
    put(float[].class, FloatArraySerializer.class);
    put(Float[].class, FloatArraySerializer.class);
    put(double.class, DoubleSerializer.class);
    put(Double.class, DoubleSerializer.class);
    put(double[].class, DoubleArraySerializer.class);
    put(Double[].class, DoubleArraySerializer.class);
    put(String.class, StringSerializer.class);
    put(Class.class, ClassSerializer.class);
    put(Enum.class, EnumSerializer.class);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    int i = 128;
    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry : SERIALIZERS.entrySet()) {
      registry.register(entry.getKey(), entry.getValue(), i++);
    }
  }

}
