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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URI;

import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.BasicRegistry;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriException;
import net.kuujo.copycat.uri.UriInject;

/**
 * URI injector.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UriInjector {
  private final URI uri;
  private final Registry registry;

  public UriInjector(URI uri) {
    this.uri = uri;
    this.registry = new BasicRegistry();
  }

  public UriInjector(URI uri, Registry registry) {
    this.uri = uri;
    this.registry = registry;
  }

  /**
   * Injects the given type with URI arguments.
   *
   * @param type The type to inject.
   * @return The injected object.
   */
  public <T> T inject(Class<T> type) {
    return injectSetters(injectConstructor(type));
  }

  /**
   * Injects the object constructor.
   */
  @SuppressWarnings("unchecked")
  private <T> T injectConstructor(Class<T> type) {
    // Find the best matching constructor for the given URI. This is done by locating
    // the first constructor with the most available arguments in the URI since ordering
    // of constructors is unreliable.
    Constructor<?> matchConstructor = null;
    Object[] matchArgs = null;
    Integer matchCount = null;
    for (Constructor<?> constructor : type.getDeclaredConstructors()) {
      if (constructor.isAnnotationPresent(UriInject.class)) {
        constructor.setAccessible(true);

        // Map parameter types to URI parameters by locating constructor parameter
        // annotations and parsing the URI to inject URI values into the constructor.
        Parameter[] params = constructor.getParameters();
        Object[] args = new Object[params.length];

        try {
          for (int i = 0; i < params.length; i++) {
            Parameter param = params[i];
  
            // Iterate through annotations in order and create annotation values by
            // parsing the URI. Whichever annotation produces a non-null value first wins.
            Object value = null;
            for (Annotation annotation : param.getAnnotations()) {
              UriInjectable injectable = annotation.annotationType().getAnnotation(UriInjectable.class);
              if (injectable != null) {
                injectable.value().getConstructor(new Class<?>[]{}).setAccessible(true);
                try {
                  Object result = injectable.value().newInstance().parse(uri, annotation, registry, param.getType());
                  if (result != null) {
                    value = result;
                    break;
                  }
                } catch (ClassCastException e) {
                  throw new UriException(e);
                }
              }
            }
  
            // If no value was found for this parameter, check to see if the parameter
            // is optional. Optional parameters will continue on with null values.
            // Otherwise, an IllegalStateException will be thrown.
            if (value == null && !param.isAnnotationPresent(Optional.class)) {
              throw new IllegalStateException("Missing argument of type " + param.getType());
            }
  
            args[i] = value;
          }
        } catch (SecurityException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
          throw new UriException(e);
        }

        // Count the number of non-null arguments for the constructor.
        int count = 0;
        for (Object arg : args) {
          if (arg != null) {
            count++;
          }
        }

        // If the number of non-null arguments is greater than that of the current
        // best constructor, use this constructor as the best match.
        if (matchCount == null || count > matchCount) {
          matchConstructor = constructor;
          matchArgs = args;
          matchCount = count;
        }
      }
    }

    // Construct the object. If no matching URI injectable constructor was
    // found then the class must declare a default constructor.
    try {
      if (matchConstructor != null) {
        return (T) matchConstructor.newInstance(matchArgs);
      } else {
        return type.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new UriException(e);
    }
  }

  /**
   * Injects the object setters.
   */
  @SuppressWarnings("unchecked")
  private <T> T injectSetters(T object) {
    try {
      // Search bean write methods for @UriInjectable annotations.
      BeanInfo info = Introspector.getBeanInfo(object.getClass());
      for (PropertyDescriptor descriptor : info.getPropertyDescriptors()) {
        Method method = descriptor.getWriteMethod();
        if (method == null) continue;

        // Iterate through the write method's annotations and search
        // for injectable annotations. The URI is parsed by injectable parsers
        // until the first non-null argument value is found.
        Object value = null;
        for (Annotation annotation : method.getAnnotations()) {
          UriInjectable injectable = annotation.annotationType().getAnnotation(UriInjectable.class);
          if (injectable != null) {
            injectable.value().getConstructor(new Class<?>[]{}).setAccessible(true);
            try {
              Object result = injectable.value().newInstance().parse(uri, annotation, registry, descriptor.getPropertyType());
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
            value = parser.parse(uri, new GenericUriQueryParam(descriptor.getName()), registry, (Class<Object>) descriptor.getPropertyType());
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
