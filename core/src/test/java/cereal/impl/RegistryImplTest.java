/*
 * Copyright 2015 Josh Elser
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
package cereal.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.Mapping;
import cereal.Registry;
import cereal.impl.InstanceOrBuilderImpl;
import cereal.impl.RegistryImpl;
import cereal.impl.objects.pojo.SimplePojo;

public class RegistryImplTest {

  private Registry registry;

  @Before
  public void setup() {
    registry = new RegistryImpl();
  }

  @Test
  public void testEmptyRegistry() {
    InstanceOrBuilder<SimplePojo> iob = new InstanceOrBuilderImpl<>(new SimplePojo());
    assertNull(registry.get(iob));
    Mapping<SimplePojo> testMapping = new Mapping<SimplePojo>() {
      @Override
      public Text getRowId(SimplePojo obj) {
        return null;
      }

      @Override
      public List<Field> getFields(SimplePojo obj) {
        return null;
      }

      @Override
      public void update(Entry<Key,Value> entry, InstanceOrBuilder<SimplePojo> obj) {}

      @Override
      public Class<SimplePojo> objectType() {
        return SimplePojo.class;
      }
    };

    registry.add(testMapping);

    assertEquals(testMapping, registry.get(iob));
  }

  @Test(expected = NullPointerException.class)
  public void testBadMapping() {
    InstanceOrBuilder<SimplePojo> iob = new InstanceOrBuilderImpl<>(new SimplePojo());
    assertNull(registry.get(iob));
    Mapping<SimplePojo> testMapping = new Mapping<SimplePojo>() {
      @Override
      public Text getRowId(SimplePojo obj) {
        return null;
      }

      @Override
      public List<Field> getFields(SimplePojo obj) {
        return null;
      }

      @Override
      public void update(Entry<Key,Value> entry, InstanceOrBuilder<SimplePojo> obj) {}

      @Override
      public Class<SimplePojo> objectType() {
        return null;
      }
    };

    registry.add(testMapping);
  }

  @Test(expected = NullPointerException.class)
  public void testNullInstOrBuilder() {
    registry.get(null);
  }

  @Test(expected = NullPointerException.class)
  public void testBadInstOrBuilder() {
    InstanceOrBuilder<SimplePojo> iob = new InstanceOrBuilder<SimplePojo>() {
      @Override
      public cereal.InstanceOrBuilder.Type getType() {
        return null;
      }
      
      @Override
      public Object get() {
        return null;
      }
      
      @Override
      public Class<SimplePojo> getWrappedClass() {
        return null;
      }
    };
    
    registry.get(iob);
  }

}
