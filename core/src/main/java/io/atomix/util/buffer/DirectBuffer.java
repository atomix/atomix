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

import io.atomix.util.memory.HeapMemory;
import io.atomix.util.memory.Memory;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBuffer extends ByteBufferBuffer {

    /**
     * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link Long#MAX_VALUE}.
     * <p>
     * When the buffer is constructed, {@link io.atomix.util.memory.DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will be initialized with a capacity of {@code 4096}
     * and have a maximum capacity of {@link Long#MAX_VALUE}. The buffer's {@code capacity} will dynamically expand as
     * bytes are written to the buffer. The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
     *
     * @return The direct buffer.
     * @see DirectBuffer#allocate(long)
     * @see DirectBuffer#allocate(long, long)
     */
    public static DirectBuffer allocate() {
        return allocate(DEFAULT_INITIAL_CAPACITY, HeapMemory.MAX_SIZE);
    }

    /**
     * Allocates a direct buffer with the given initial capacity.
     * <p>
     * When the buffer is constructed, {@link io.atomix.util.memory.DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code capacity}.
     * The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
     *
     * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
     * @return The direct buffer.
     * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed count for
     *                                  a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
     * @see DirectBuffer#allocate()
     * @see DirectBuffer#allocate(long, long)
     */
    public static DirectBuffer allocate(long initialCapacity) {
        return allocate(initialCapacity, HeapMemory.MAX_SIZE);
    }

    /**
     * Allocates a new direct buffer.
     * <p>
     * When the buffer is constructed, {@link io.atomix.util.memory.DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code initialCapacity}
     * and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The underlying {@link UnsafeDirectBytes}
     * will be initialized to the next power of {@code 2}.
     *
     * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
     * @param maxCapacity     The maximum capacity of the buffer.
     * @return The direct buffer.
     * @throws IllegalArgumentException If {@code capacity} or {@code maxCapacity} is greater than the maximum
     *                                  allowed count for a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
     * @see DirectBuffer#allocate()
     * @see DirectBuffer#allocate(long)
     */
    public static DirectBuffer allocate(long initialCapacity, long maxCapacity) {
        if (initialCapacity > maxCapacity)
            throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
        return new DirectBuffer(DirectBytes.allocate((int) Math.min(Memory.Util.toPow2(initialCapacity), HeapMemory.MAX_SIZE)), 0, initialCapacity, maxCapacity);
    }

    protected DirectBuffer(DirectBytes bytes, long offset, long initialCapacity, long maxCapacity) {
        super(bytes, offset, initialCapacity, maxCapacity, null);
    }

    @Override
    public DirectBuffer duplicate() {
        return new DirectBuffer((DirectBytes) bytes, offset(), capacity(), maxCapacity());
    }
}
