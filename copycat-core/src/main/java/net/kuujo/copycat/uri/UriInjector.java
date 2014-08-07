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

import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.EndpointException;

/**
 * Handles injection of URI parts into new or existing objects.<p>
 *
 * The URI injector uses constructor, method and parameter annotations
 * to determine which URI values should be injected in place of which
 * parameters via which methods.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UriInjector {
  private final URI uri;
  private final Map<String, Object> args = new HashMap<>();

  public UriInjector(URI uri, CopyCatContext context) {
    this.uri = uri;
    String query = uri.getQuery();
    if (query != null) {
      String[] pairs = query.split("&");
      for (String pair : pairs) {
        int index = pair.indexOf("=");
        try {
          String key = URLDecoder.decode(pair.substring(0, index), "UTF-8");
          String value = URLDecoder.decode(pair.substring(index + 1), "UTF-8");
          if (value.startsWith("#")) {
            args.put(key, context.registry().lookup(value.substring(1)));
          } else {
            args.put(key, value);
          }
        } catch (UnsupportedEncodingException e) {
          throw new EndpointException(e);
        }
      }
    }
  }

  /**
   * Creates a new instance of the given type.
   */
  @SuppressWarnings("unchecked")
  public <T> T inject(Class<T> type) {
    T object = null;

    // First, attempt to create the object from a @UriInject annotated constructor.
    for (Constructor<?> constructor : type.getDeclaredConstructors()) {
      if (constructor.isAnnotationPresent(UriInject.class)) {
        constructor.setAccessible(true);
        Object[] args;
        try {
          args = buildArguments(constructor.getParameters());
        } catch (IllegalStateException e) {
          continue;
        }
        try {
          object = (T) constructor.newInstance(args);
          break;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new UriException(e);
        }
      }
    }

    // If no object could be constructed (no @UriInject annotation was found) then
    // attempt to create the object from a no-argument constructor.
    if (object == null) {
      try {
        object = type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new UriException(e);
      }
    }

    // Once the object has been constructed, inject additional URI values
    // via annotated methods if available.
    return inject(object);
  }

  /**
   * Injects the given object with values from the URI.
   *
   * @param object The object to inject.
   * @return The injected object.
   */
  public <T> T inject(T object) {
    Class<?> type = object.getClass();
    Set<String> injectedMethods = new HashSet<>();
    while (type != Object.class) {
      for (Method method : type.getDeclaredMethods()) {
        if (method.isAnnotationPresent(UriInject.class) && !injectedMethods.contains(String.format("%s%s", method.getName(), method.getParameterTypes()))) {
          Object[] args = buildArguments(method.getParameters());
          method.setAccessible(true);
          try {
            method.invoke(object, args);
          } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new UriException(e);
          }
        }
      }
      type = type.getSuperclass();
    }
    return object;
  }

  /**
   * Builds an array of arguments given a set of parameters.
   *
   * @param parameters An array of parameters from which to build arguments.
   * @return An array arguments for the given parameters.
   */
  private Object[] buildArguments(Parameter[] parameters) throws IllegalStateException {
    Object[] args = new Object[parameters.length];
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      Object value = null;
      for (Annotation annotation : parameter.getAnnotations()) {
        if (annotation.annotationType().isAnnotationPresent(UriInject.class)) {
          if (annotation instanceof UriScheme) {
            String scheme = uri.getScheme();
            if (scheme != null) {
              value = scheme;
              break;
            }
          } else if (annotation instanceof UriSchemeSpecificPart) {
            String schemeSpecificPart = uri.getSchemeSpecificPart();
            if (schemeSpecificPart != null) {
              value = schemeSpecificPart;
              break;
            }
          } else if (annotation instanceof UriUserInfo) {
            String userInfo = uri.getUserInfo();
            if (userInfo != null) {
              value = userInfo;
              break;
            }
          } else if (annotation instanceof UriHost) {
            String host = uri.getHost();
            if (host != null) {
              value = host;
              break;
            }
          } else if (annotation instanceof UriPort) {
            int port = uri.getPort();
            if (port >= 0) {
              value = port;
              break;
            }
          } else if (annotation instanceof UriAuthority) {
            String authority = uri.getAuthority();
            if (authority != null) {
              value = authority;
              break;
            }
          } else if (annotation instanceof UriPath) {
            String path = uri.getPath();
            if (path != null) {
              value = path;
              break;
            }
          } else if (annotation instanceof UriQuery) {
            String query = uri.getQuery();
            if (query != null) {
              value = query;
              break;
            }
          } else if (annotation instanceof UriFragment) {
            String fragment = uri.getFragment();
            if (fragment != null) {
              value = fragment;
              break;
            }
          } else if (annotation instanceof UriArgument) {
            Object arg = this.args.get(((UriArgument) annotation).value());
            if (arg != null) {
              value = arg;
              break;
            }
          }
        }
      }

      // If no value was found for this parameter, check to see if the parameter
      // is optional. Optional parameters will continue on with null values.
      // Otherwise, an IllegalStateException will be thrown.
      if (value == null) {
        Optional optional = parameter.getAnnotation(Optional.class);
        if (optional == null) {
          throw new IllegalStateException("Missing argument of type " + parameter.getType());
        }
      }

      args[i] = value;
    }
    return args;
  }

}
