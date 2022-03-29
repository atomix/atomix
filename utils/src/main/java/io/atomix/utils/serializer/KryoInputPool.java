// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.io.Input;

class KryoInputPool extends KryoIOPool<Input> {

  static final int MAX_POOLED_BUFFER_SIZE = 512 * 1024;

  @Override
  protected Input create(int bufferSize) {
    return new Input(bufferSize);
  }

  @Override
  protected boolean recycle(Input input) {
    if (input.getBuffer().length < MAX_POOLED_BUFFER_SIZE) {
      input.setInputStream(null);
      return true;
    }
    return false; // discard
  }
}
