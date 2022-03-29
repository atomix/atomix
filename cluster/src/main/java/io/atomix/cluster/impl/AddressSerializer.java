// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.utils.net.Address;

/**
 * Address serializer.
 */
public class AddressSerializer extends com.esotericsoftware.kryo.Serializer<Address> {
  @Override
  public void write(Kryo kryo, Output output, Address address) {
    output.writeString(address.host());
    output.writeInt(address.port());
  }

  @Override
  public Address read(Kryo kryo, Input input, Class<Address> type) {
    String host = input.readString();
    int port = input.readInt();
    return Address.from(host, port);
  }
}
