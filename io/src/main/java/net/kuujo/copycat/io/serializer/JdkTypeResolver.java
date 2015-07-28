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

import java.io.Externalizable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * JDK utilities type resolver.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JdkTypeResolver implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final HashMap<Class<?>, Class<? extends TypeSerializer<?>>> SERIALIZERS = new LinkedHashMap() {{
    put(BigInteger.class, BigIntegerSerializer.class);
    put(BigDecimal.class, BigDecimalSerializer.class);
    put(Date.class, DateSerializer.class);
    put(Calendar.class, CalendarSerializer.class);
    put(TimeZone.class, TimeZoneSerializer.class);
    put(Map.class, MapSerializer.class);
    put(Set.class, SetSerializer.class);
    put(List.class, ListSerializer.class);
    put(Externalizable.class, ExternalizableSerializer.class);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    int i = 176;
    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry : SERIALIZERS.entrySet()) {
      registry.register(entry.getKey(), entry.getValue(), i++);
    }
  }

}
