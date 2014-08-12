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

import java.lang.annotation.Annotation;
import java.net.URI;

import net.kuujo.copycat.registry.Registry;

/**
 * Parser for parsing an injectable URI argument.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface UriParser<U extends Annotation, T> {

  /**
   * Parses an argument from the URI.
   *
   * @param uri The URI from which to parse the argument.
   * @param annotation The annotation being parsed.
   * @param registry The current local registry.
   * @param type The expected return value type.
   * @return The parsed argument.
   */
  T parse(URI uri, U annotation, Registry registry, Class<T> type);

  /**
   * Placeholder implementation which represents no URI parser.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class None implements UriParser<UriHost, Object> {
    @Override
    public Object parse(URI uri, UriHost annotation, Registry registry, Class<Object> type) {
      throw new UnsupportedOperationException();
    }
  }

}
