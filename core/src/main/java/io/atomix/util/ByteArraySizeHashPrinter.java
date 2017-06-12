/*
 * Copyright 2014-present Open Networking Laboratory
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

package io.atomix.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Arrays;

/**
 * Helper to print byte[] length and hashCode.
 */
public final class ByteArraySizeHashPrinter {

    private final byte[] bytes;

    /**
     * Returns ByteArraySizeHashPrinter wrapping given byte[].
     *
     * @param bytes bytes to wrap around
     * @return ByteArraySizeHashPrinter
     */
    public static ByteArraySizeHashPrinter of(byte[] bytes) {
        return new ByteArraySizeHashPrinter(bytes);
    }

    /**
     * Returns ByteArraySizeHashPrinter wrapping given byte[].
     *
     * @param bytes bytes to wrap around
     * @return null if {@code bytes == null}, ByteArraySizeHashPrinter otherwise
     */
    public static ByteArraySizeHashPrinter orNull(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new ByteArraySizeHashPrinter(bytes);
    }

    public ByteArraySizeHashPrinter(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper("byte[]");
        if (bytes != null) {
            helper.add("length", bytes.length)
                  .add("hash", Arrays.hashCode(bytes));
        } else {
            helper.addValue(bytes);
        }
        return helper.toString();
    }
}
