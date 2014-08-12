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
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.net.URLDecoder;

import net.kuujo.copycat.registry.Registry;

/**
 * Named URI query argument injector annotation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@UriInjectable(UriQueryParam.Parser.class)
@Target({ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface UriQueryParam {

  /**
   * The argument name.
   */
  String value();

  /**
   * Query parameter parser.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Parser<T> implements UriParser<UriQueryParam, T> {
    private static final String QUERY_DELIMITER = "&";
    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String ENCODING = "UTF-8";
    private static final String VARIABLE_PREFIX = "$";

    @Override
    @SuppressWarnings("unchecked")
    public T parse(URI uri, UriQueryParam annotation, Registry registry, Class<T> type) {
      String query = uri.getQuery();
      if (query != null) {
        String[] pairs = query.split(QUERY_DELIMITER);
        for (String pair : pairs) {
          int index = pair.indexOf(KEY_VALUE_SEPARATOR);
          try {
            String key = URLDecoder.decode(pair.substring(0, index), ENCODING);
            String value = URLDecoder.decode(pair.substring(index + 1), ENCODING);
            if (key.equals(annotation.value())) {
              if (value == null) {
                return null;
              } else if (value.startsWith(VARIABLE_PREFIX)) {
                return (T) registry.lookup(value.substring(1), type);
              } else if (String.class.isAssignableFrom(type)) {
                return (T) String.valueOf(value);
              } else if (int.class.isAssignableFrom(type) || Integer.class.isAssignableFrom(type)) {
                return (T) Integer.valueOf(value);
              } else if (short.class.isAssignableFrom(type) || Short.class.isAssignableFrom(type)) {
                return (T) Short.valueOf(value);
              } else if (long.class.isAssignableFrom(type) || Long.class.isAssignableFrom(type)) {
                return (T) Long.valueOf(value);
              } else if (float.class.isAssignableFrom(type) || Float.class.isAssignableFrom(type)) {
                return (T) Float.valueOf(value);
              } else if (double.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type)) {
                return (T) Double.valueOf(value);
              } else if (byte.class.isAssignableFrom(type) || Byte.class.isAssignableFrom(type)) {
                return (T) Byte.valueOf(value);
              } else if (char.class.isAssignableFrom(type) || Character.class.isAssignableFrom(type)) {
                return (T) Character.valueOf(value.charAt(0));
              } else if (boolean.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type)) {
                if (value.equals("true") || value.equals("1")) {
                  return type.cast(true);
                } else if (value.equals("false") || value.equals("0")) {
                  return type.cast(false);
                } else {
                  return (T) value;
                }
              } else {
                return (T) value;
              }
            }
          } catch (UnsupportedEncodingException e) {
            throw new UriException(e);
          }
        }
      }
      return null;
    }
    
  }

}
