// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableSet;

/**
 * Kryo Serializer for {@link ImmutableSet}.
 */
public class ImmutableSetSerializer extends Serializer<ImmutableSet<?>> {

  /**
   * Creates {@link ImmutableSet} serializer instance.
   */
  public ImmutableSetSerializer() {
    // non-null, immutable
    super(false, true);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableSet<?> object) {
    output.writeInt(object.size());
    for (Object e : object) {
      kryo.writeClassAndObject(output, e);
    }
  }

  @Override
  public ImmutableSet<?> read(Kryo kryo, Input input,
      Class<ImmutableSet<?>> type) {
    final int size = input.readInt();
    switch (size) {
      case 0:
        return ImmutableSet.of();
      case 1:
        return ImmutableSet.of(kryo.readClassAndObject(input));
      default:
        Object[] elms = new Object[size];
        for (int i = 0; i < size; ++i) {
          elms[i] = kryo.readClassAndObject(input);
        }
        return ImmutableSet.copyOf(elms);
    }
  }
}
