/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.io.Output;

class KryoOutputPool extends KryoIOPool<Output> {

    private static final int MAX_BUFFER_SIZE = 768 * 1024;
    static final int MAX_POOLED_BUFFER_SIZE = 512 * 1024;

    @Override
    protected Output create(int bufferSize) {
        return new Output(bufferSize, MAX_BUFFER_SIZE);
    }

    @Override
    protected boolean recycle(Output output) {
        if (output.getBuffer().length < MAX_POOLED_BUFFER_SIZE) {
            output.clear();
            return true;
        }
        return false; // discard
    }
}
