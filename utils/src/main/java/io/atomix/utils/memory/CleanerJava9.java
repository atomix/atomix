/*
* Copyright 2017 The Netty Project
*
* The Netty Project licenses this file to you under the Apache License,
* version 2.0 (the "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at:
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*/
package io.atomix.utils.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Provide a way to clean a ByteBuffer on Java9+.
 */
final class CleanerJava9 implements Cleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanerJava9.class);

    private static final Method INVOKE_CLEANER;

    static {
        final Method method;
        final Throwable error;
        ByteBuffer buffer = ByteBuffer.allocateDirect(1);
        Object maybeInvokeMethod;
        try {
            // See https://bugs.openjdk.java.net/browse/JDK-8171377
            Method m = NativeMemory.UNSAFE.getClass().getDeclaredMethod("invokeCleaner", ByteBuffer.class);
            m.invoke(NativeMemory.UNSAFE, buffer);
            maybeInvokeMethod = m;
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            maybeInvokeMethod = e;
        }
        if (maybeInvokeMethod instanceof Throwable) {
            method = null;
            error = (Throwable) maybeInvokeMethod;
        } else {
            method = (Method) maybeInvokeMethod;
            error = null;
        }
        if (error == null) {
            LOGGER.debug("java.nio.ByteBuffer.cleaner(): available");
        } else {
            LOGGER.debug("java.nio.ByteBuffer.cleaner(): unavailable", error);
        }
        INVOKE_CLEANER = method;
    }

    static boolean isSupported() {
        return INVOKE_CLEANER != null;
    }

    @Override
    public void freeDirectBuffer(ByteBuffer buffer) {
        try {
            INVOKE_CLEANER.invoke(NativeMemory.UNSAFE, buffer);
        } catch (Throwable cause) {
            NativeMemory.UNSAFE.throwException(cause);
        }
    }
}
