/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.logging;

/**
 * Logger.
 */
public interface Logger {

    String getName();

    boolean isTraceEnabled();

    void trace(String message);

    void trace(String message, Object arg);

    void trace(String message, Object arg1, Object arg2);

    void trace(String message, Object... args);

    void trace(String message, Throwable t);

    boolean isDebugEnabled();

    void debug(String message);

    void debug(String message, Object arg);

    void debug(String message, Object arg1, Object arg2);

    void debug(String message, Object... args);

    void debug(String message, Throwable t);

    boolean isInfoEnabled();

    void info(String message);

    void info(String message, Object arg1);

    void info(String message, Object arg1, Object arg2);

    void info(String message, Object... args);

    void info(String message, Throwable t);

    boolean isWarnEnabled();

    void warn(String message);

    void warn(String message, Object arg1);

    void warn(String message, Object... args);

    void warn(String message, Object arg1, Object arg2);

    void warn(String message, Throwable t);

    boolean isErrorEnabled();

    void error(String message);

    void error(String message, Object arg1);

    void error(String message, Object arg1, Object arg2);

    void error(String message, Object... args);

    void error(String message, Throwable t);

}
