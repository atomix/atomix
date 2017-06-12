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
package io.atomix.logging.impl;

import io.atomix.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 * SLF4J logger.
 */
public class Slf4jLogger implements Logger {
    private final org.slf4j.Logger logger;

    public Slf4jLogger(String name) {
        this(LoggerFactory.getLogger(name));
    }

    public Slf4jLogger(Class<?> clazz) {
        this(LoggerFactory.getLogger(clazz));
    }

    public Slf4jLogger(org.slf4j.Logger logger) {
        this.logger = logger;
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String message) {
        logger.trace(message);
    }

    @Override
    public void trace(String message, Object arg) {
        logger.trace(message, arg);
    }

    @Override
    public void trace(String message, Object arg1, Object arg2) {
        logger.trace(message, arg1, arg2);
    }

    @Override
    public void trace(String message, Object... args) {
        logger.trace(message, args);
    }

    @Override
    public void trace(String message, Throwable t) {
        logger.trace(message, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        logger.debug(message);
    }

    @Override
    public void debug(String message, Object arg) {
        logger.debug(message, arg);
    }

    @Override
    public void debug(String message, Object arg1, Object arg2) {
        logger.debug(message, arg1, arg2);
    }

    @Override
    public void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    @Override
    public void debug(String message, Throwable t) {
        logger.debug(message, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String message) {
        logger.info(message);
    }

    @Override
    public void info(String message, Object arg1) {
        logger.info(message, arg1);
    }

    @Override
    public void info(String message, Object arg1, Object arg2) {
        logger.info(message, arg1, arg2);
    }

    @Override
    public void info(String message, Object... args) {
        logger.info(message, args);
    }

    @Override
    public void info(String message, Throwable t) {
        logger.info(message, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String message) {
        logger.warn(message);
    }

    @Override
    public void warn(String message, Object arg1) {
        logger.warn(message, arg1);
    }

    @Override
    public void warn(String message, Object... args) {
        logger.warn(message, args);
    }

    @Override
    public void warn(String message, Object arg1, Object arg2) {
        logger.warn(message, arg1, arg2);
    }

    @Override
    public void warn(String message, Throwable t) {
        logger.warn(message, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String message) {
        logger.error(message);
    }

    @Override
    public void error(String message, Object arg1) {
        logger.error(message, arg1);
    }

    @Override
    public void error(String message, Object arg1, Object arg2) {
        logger.error(message, arg1, arg2);
    }

    @Override
    public void error(String message, Object... args) {
        logger.error(message, args);
    }

    @Override
    public void error(String message, Throwable t) {
        logger.error(message, t);
    }
}
