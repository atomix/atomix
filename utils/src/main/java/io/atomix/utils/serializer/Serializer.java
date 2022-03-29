// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.utils.serializer;

/**
 * Interface for serialization of store artifacts.
 */
public interface Serializer {

  /**
   * Creates a new serializer builder.
   *
   * @return a new serializer builder
   */
  static SerializerBuilder builder() {
    return new SerializerBuilder();
  }

  /**
   * Creates a new serializer builder.
   *
   * @param name the serializer name
   * @return a new serializer builder
   */
  static SerializerBuilder builder(String name) {
    return new SerializerBuilder(name);
  }

  /**
   * Serialize the specified object.
   *
   * @param object object to serialize.
   * @param <T>    encoded type
   * @return serialized bytes.
   */
  <T> byte[] encode(T object);

  /**
   * Deserialize the specified bytes.
   *
   * @param bytes byte array to deserialize.
   * @param <T>   decoded type
   * @return deserialized object.
   */
  <T> T decode(byte[] bytes);

  /**
   * Creates a new Serializer instance from a Namespace.
   *
   * @param namespace serializer namespace
   * @return Serializer instance
   */
  static Serializer using(Namespace namespace) {
    return new Serializer() {
      @Override
      public <T> byte[] encode(T object) {
        return namespace.serialize(object);
      }

      @Override
      public <T> T decode(byte[] bytes) {
        return namespace.deserialize(bytes);
      }
    };
  }

}
