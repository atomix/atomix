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
package net.kuujo.copycat.uri;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

import net.kuujo.copycat.registry.Registry;

/**
 * URI injector.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UriInjector {
  private final URI uri;
  private final Registry registry;

  public UriInjector(URI uri, Registry registry) {
    this.uri = uri;
    this.registry = registry;
  }

  /**
   * Injects the given class with URI arguments via annotated bean properties.
   *
   * @param type The type to inject.
   * @return The injected object.
   */
  public <T> T inject(Class<T> type) {
    try {
      return inject(type.newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new UriException(e);
    }
  }

  /**
   * Injects the given object with URI arguments via annotated bean properties.
   *
   * @param object The object to inject.
   * @return The injected object.
   */
  @SuppressWarnings("unchecked")
  public <T> T inject(T object) {
    try {
      // Iterate through fields and attempt to locate annotated fields. This supports
      // directly setting annotated field values without declaring setters.
      Class<?> clazz = object.getClass();
      while (clazz != Object.class) {
        outer:
          for (Field field : clazz.getDeclaredFields()) {
            for (Annotation annotation : field.getAnnotations()) {
              UriInjectable injectable = annotation.annotationType().getAnnotation(UriInjectable.class);
              if (injectable != null) {
                try {
                  field.setAccessible(true);
                  field.set(object, injectable.value().newInstance().parse(uri, annotation, registry, field.getType()));
                } catch (ClassCastException e) {
                  throw new UriException(e);
                }
                continue outer;
              }
            }

            // If the field is a Registry type field then set it.
            if (field.getType() == Registry.class) {
              field.setAccessible(true);
              field.set(object, registry);
            }
        }
        clazz = clazz.getSuperclass();
      }

      // Search bean write methods for @UriInjectable annotations.
      BeanInfo info = Introspector.getBeanInfo(object.getClass());

      for (PropertyDescriptor property : info.getPropertyDescriptors()) {
        Method method = property.getWriteMethod();
        if (method == null) continue;

        // If the property type is Registry then just set the registry.
        if (Registry.class.isAssignableFrom(property.getPropertyType())) {
          method.invoke(object, registry);
        }

        // Iterate through the write method's annotations and search
        // for injectable annotations. The URI is parsed by injectable parsers
        // until the first non-null argument value is found.
        Object value = null;
        for (Annotation annotation : method.getAnnotations()) {
          UriInjectable injectable = annotation.annotationType().getAnnotation(UriInjectable.class);
          if (injectable != null) {
            injectable.value().getConstructor(new Class<?>[]{}).setAccessible(true);
            try {
              Object result = injectable.value().newInstance().parse(uri, annotation, registry, property.getPropertyType());
              if (result != null) {
                value = result;
                break;
              }
            } catch (ClassCastException e) {
              throw new UriException(e);
            }
          }
        }

        // If no non-null value was found then attempt to apply a named query parameter.
        if (value == null) {
          UriParser<UriQueryParam, Object> parser = new UriQueryParam.Parser<Object>();
          try {
            value = parser.parse(uri, new GenericUriQueryParam(property.getName()), registry, (Class<Object>) property.getPropertyType());
          } catch (ClassCastException e) {
            throw new UriException(e);
          }
        }

        // If an argument value was found, apply the value to the object via the bean setter.
        if (value != null) {
          method.invoke(object, value);
        }
      }
    } catch (IntrospectionException | IllegalStateException | IllegalAccessException
        | InstantiationException | NoSuchMethodException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new UriException(e);
    }

    return object;
  }

  /**
   * Generic URI query parameter that can be dynamically constructed.
   */
  private static class GenericUriQueryParam implements Annotation, UriQueryParam {
    private final String name;

    private GenericUriQueryParam(String name) {
      this.name = name;
    }

    @Override
    public String value() {
      return name;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
      return UriQueryParam.class;
    }
  }

}
