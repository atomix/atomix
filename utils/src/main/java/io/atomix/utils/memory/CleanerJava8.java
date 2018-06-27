/*
* Copyright 2014 The Netty Project
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Allows to free direct {@link ByteBuffer} by using Cleaner.
 */
final class CleanerJava8 implements Cleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanerJava8.class);

    private static final long CLEANER_FIELD_OFFSET;
    private static final Method CLEAN_METHOD;

    static {
        long fieldOffset = -1;
        Method clean = null;
        Throwable error = null;
        ByteBuffer direct = ByteBuffer.allocateDirect(1);
        try {
            Field cleanerField = direct.getClass().getDeclaredField("cleaner");
            fieldOffset = NativeMemory.UNSAFE.objectFieldOffset(cleanerField);
            Object cleaner = NativeMemory.UNSAFE.getObject(direct, fieldOffset);
            clean = cleaner.getClass().getDeclaredMethod("clean");
            clean.invoke(cleaner);
        } catch (Throwable t) {
            // We don't have ByteBuffer.cleaner().
            fieldOffset = -1;
            clean = null;
            error = t;
        }
        if (error == null) {
            LOGGER.debug("java.nio.ByteBuffer.cleaner(): available");
        } else {
            LOGGER.debug("java.nio.ByteBuffer.cleaner(): unavailable", error);
        }
        CLEANER_FIELD_OFFSET = fieldOffset;
        CLEAN_METHOD = clean;
    }

    static boolean isSupported() {
        return CLEANER_FIELD_OFFSET != -1;
    }

    @Override
    public void freeDirectBuffer(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            return;
        }
        try {
            Object cleaner = NativeMemory.UNSAFE.getObject(buffer, CLEANER_FIELD_OFFSET);
            if (cleaner != null) {
                CLEAN_METHOD.invoke(cleaner);
            }
        } catch (Throwable cause) {
            NativeMemory.UNSAFE.throwException(cause);
        }
    }
}
