/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.logging;

/**
 * Logging service.
 */
public interface LoggingService {

  /**
   * Returns a logger by name.
   *
   * @param name the logger name
   * @return the logger
   */
  Logger getLogger(String name);

  /**
   * Returns a logger by class.
   *
   * @param clazz the class for which to return the logger
   * @return the logger for the given class
   */
  Logger getLogger(Class<?> clazz);

}
