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
package io.atomix.util.buffer;

import io.atomix.catalyst.util.reference.ReferenceManager;

/**
 * {@link java.nio.ByteBuffer} based buffer.
 */
public abstract class ByteBufferBuffer extends AbstractBuffer {
    protected final ByteBufferBytes bytes;

    public ByteBufferBuffer(ByteBufferBytes bytes, ReferenceManager<Buffer> referenceManager) {
        super(bytes, referenceManager);
        this.bytes = bytes;
    }

    public ByteBufferBuffer(ByteBufferBytes bytes, long offset, long initialCapacity, long maxCapacity, ReferenceManager<Buffer> referenceManager) {
        super(bytes, offset, initialCapacity, maxCapacity, referenceManager);
        this.bytes = bytes;
    }

    @Override
    public byte[] array() {
        return bytes.array();
    }

    @Override
    protected void compact(long from, long to, long length) {
        byte[] bytes = new byte[1024];
        long position = from;
        while (position < from + length) {
            long size = Math.min((from + length) - position, 1024);
            this.bytes.read(position, bytes, 0, size);
            this.bytes.write(0, bytes, 0, size);
            position += size;
        }
    }

}
