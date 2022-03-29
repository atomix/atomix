// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Generics utility.
 */
public class Generics {

  /**
   * Returns the generic type at the given position for the given class.
   *
   * @param instance the implementing instance
   * @param clazz    the generic class
   * @param position the generic position
   * @return the generic type at the given position
   */
  public static Type getGenericClassType(Object instance, Class<?> clazz, int position) {
    Class<?> type = instance.getClass();
    while (type != Object.class) {
      if (type.getGenericSuperclass() instanceof ParameterizedType) {
        ParameterizedType genericSuperclass = (ParameterizedType) type.getGenericSuperclass();
        if (genericSuperclass.getRawType() == clazz) {
          return genericSuperclass.getActualTypeArguments()[position];
        } else {
          type = type.getSuperclass();
        }
      } else {
        type = type.getSuperclass();
      }
    }
    return null;
  }

  /**
   * Returns the generic type at the given position for the given interface.
   *
   * @param instance the implementing instance
   * @param iface    the generic interface
   * @param position the generic position
   * @return the generic type at the given position
   */
  public static Type getGenericInterfaceType(Object instance, Class<?> iface, int position) {
    Class<?> type = instance.getClass();
    while (type != Object.class) {
      for (Type genericType : type.getGenericInterfaces()) {
        if (genericType instanceof ParameterizedType) {
          ParameterizedType parameterizedType = (ParameterizedType) genericType;
          if (parameterizedType.getRawType() == iface) {
            return parameterizedType.getActualTypeArguments()[position];
          }
        }
      }
      type = type.getSuperclass();
    }
    return null;
  }

  private Generics() {
  }
}
