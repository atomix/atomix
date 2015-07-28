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

import net.kuujo.copycat.CopycatException;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Serializable type resolver based on {@link java.util.ServiceLoader}.
 * <p>
 * This type resolver resolves serializable types and serializers via {@link java.util.ServiceLoader}. It searches the
 * classpath for serializable types and serializers registered as implementations of {@link CopycatSerializable},
 * {@link TypeSerializer}, or {@link TypeSerializerFactory}. Serializables can be annotated
 * with the {@link SerializeWith} annotation to identify serializable type IDs and serializers, and
 * serializers and factories can be annotated with the {@link Serialize} annotation to identify
 * serializable types and IDs.
 * <p>
 * All classes that are loadable as services must provide a default no-argument constructor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServiceLoaderResolver implements SerializableTypeResolver {

  @Override
  public void resolve(SerializerRegistry registry) {
    resolveFactories(registry);
    resolveSerializers(registry);
    resolveSerializables(registry);
  }

  /**
   * Loads a list of classes from the classpath.
   */
  @SuppressWarnings("unchecked")
  private <T> List<Class<? extends T>> load(Class<T> clazz) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Enumeration<URL> urls;
    try {
      urls = cl.getResources(String.format("META-INF%sservices%s%s", File.separator, File.separator, clazz.getName()));
    } catch (IOException e) {
      throw new CopycatException(e);
    }

    List<Class<? extends T>> classes = new ArrayList<>();
    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      try {
        InputStream is = url.openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
          int comment = line.lastIndexOf('#');
          if (comment >= 0) line = line.substring(0, line.lastIndexOf('#'));
          line = line.trim();
          if (line.length() > 0) {
            try {
              classes.add((Class<? extends T>) cl.loadClass(line));
            } catch (ClassNotFoundException e) {
              throw new CopycatException("failed to load class: " + line, e);
            }
          }
        }
        is.close();
      } catch (IOException | IllegalArgumentException | SecurityException e) {
        throw new CopycatException("failed to read services", e);
      }
    }
    return classes;
  }

  /**
   * Resolves serializer factories.
   */
  private void resolveFactories(SerializerRegistry registry) {
    for (Class<? extends TypeSerializerFactory> factory : this.load(TypeSerializerFactory.class)) {
      Serialize serialize = factory.getAnnotation(Serialize.class);
      if (serialize != null) {
        for (Serialize.Type serializeType : serialize.value()) {
          if (serializeType.id() != -1) {
            try {
              registry.register(serializeType.type(), factory.newInstance(), serializeType.id());
            } catch (RegistrationException e) {
              // Serializer already registered.
            } catch (InstantiationException | IllegalAccessException e) {
              throw new CopycatException("failed to instantiate serializer factory: " + factory, e);
            }
          } else {
            try {
              registry.register(serializeType.type(), factory.newInstance());
            } catch (RegistrationException e) {
              // Serializer already registered.
            } catch (InstantiationException | IllegalAccessException e) {
              throw new CopycatException("failed to instantiate serializer factory: " + factory, e);
            }
          }
        }
      }
    }
  }

  /**
   * Resolves serializers.
   */
  @SuppressWarnings("unchecked")
  private void resolveSerializers(SerializerRegistry registry) {
    for (Class<? extends TypeSerializer> serializer : load(TypeSerializer.class)) {
      Serialize serialize = serializer.getAnnotation(Serialize.class);
      if (serialize != null) {
        for (Serialize.Type serializeType : serialize.value()) {
          if (serializeType.id() != -1) {
            try {
              registry.register(serializeType.type(), serializer, serializeType.id());
            } catch (RegistrationException e) {
              // Serializer already registered.
            }
          } else {
            try {
              registry.register(serializeType.type(), serializer);
            } catch (RegistrationException e) {
              // Serializer already registered.
            }
          }
        }
      }
    }
  }

  /**
   * Resolves serializables.
   */
  private void resolveSerializables(SerializerRegistry registry) {
    for (Class<? extends CopycatSerializable> serializable : load(CopycatSerializable.class)) {
      SerializeWith serializeWith = serializable.getAnnotation(SerializeWith.class);
      if (serializeWith != null) {
        if (serializeWith.serializer() != null && serializeWith.id() != -1) {
          try {
            registry.register(serializable, serializeWith.serializer(), serializeWith.id());
          } catch (RegistrationException e) {
            // Serializer already registered.
          }
        } else if (serializeWith.serializer() != null) {
          try {
            registry.register(serializable, serializeWith.serializer());
          } catch (RegistrationException e) {
            // Serializer already registered.
          }
        } else if (serializeWith.id() != -1) {
          try {
            registry.register(serializable, serializeWith.id());
          } catch (RegistrationException e) {
            // Serializer already registered.
          }
        } else {
          try {
            registry.register(serializable);
          } catch (RegistrationException e) {
            // Serializer already registered.
          }
        }
      } else {
        try {
          registry.register(serializable);
        } catch (RegistrationException e) {
          // Serializer already registered.
        }
      }
    }
  }

}
