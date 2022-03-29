// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Generics test.
 */
public class GenericsTest {
  @Test
  public void testGetInterfaceType() throws Exception {
    assertEquals(String.class, Generics.getGenericInterfaceType(new ConcreteInterface(), GenericInterface.class, 0));
    assertEquals(SomeClass.class, Generics.getGenericInterfaceType(new ConcreteInterface(), GenericInterface.class, 1));
  }

  @Test
  public void testGetClassType() throws Exception {
    assertEquals(SomeClass.class, Generics.getGenericClassType(new ConcreteClass(), GenericClass.class, 0));
  }

  public interface GenericInterface<T1, T2> {
    T1 type1();

    T2 type2();
  }

  public static class ConcreteInterface implements GenericInterface<String, SomeClass> {
    @Override
    public String type1() {
      return null;
    }

    @Override
    public SomeClass type2() {
      return null;
    }
  }

  public abstract class GenericClass<T> {
    public abstract T type();
  }

  public class ConcreteClass extends GenericClass<SomeClass> {
    @Override
    public SomeClass type() {
      return null;
    }
  }

  public class SomeClass {
  }
}
