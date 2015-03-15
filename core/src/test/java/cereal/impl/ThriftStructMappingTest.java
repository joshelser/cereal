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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.meta_data.FieldMetaData;
import org.junit.Before;
import org.junit.Test;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.impl.objects.thrift.TComplex;
import cereal.impl.objects.thrift.TSimple;

import com.google.common.collect.ImmutableMap;

public class ThriftStructMappingTest {
  private static final Text EMPTY = new Text(new byte[0]);
  private static final ColumnVisibility EMPTY_CV = new ColumnVisibility("");

  private TSimple msg;
  private TSimpleMessageMapping mapping;

  private static class TSimpleMessageMapping extends ThriftStructMapping<TSimple> {
    @Override
    public Text getRowId(TSimple obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<TSimple> objectType() {
      return TSimple.class;
    }

    @Override
    public Text getGrouping(FieldMetaData field) {
      return EMPTY;
    }

    @Override
    public ColumnVisibility getVisibility(FieldMetaData field) {
      return EMPTY_CV;
    }
  }

  private static class TComplexMessageMapping extends ThriftStructMapping<TComplex> {
    @Override
    public Text getRowId(TComplex obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<TComplex> objectType() {
      return TComplex.class;
    }

    @Override
    public Text getGrouping(FieldMetaData field) {
      return EMPTY;
    }

    @Override
    public ColumnVisibility getVisibility(FieldMetaData field) {
      return EMPTY_CV;
    }
  }

  private static class TSpecialMessageMapping extends TSimpleMessageMapping {
    @Override
    public Text getGrouping(FieldMetaData field) {
      // Grouping is the field character of the field name
      return new Text(field.fieldName.substring(0, 1));
    }

    @Override
    public ColumnVisibility getVisibility(FieldMetaData field) {
      // Visibility is the java type name
      return new ColumnVisibility(field.fieldName.substring(1, 2));
    }
  }

  private Text text(String str) {
    return new Text(str);
  }

  private ColumnVisibility visibility(String str) {
    return new ColumnVisibility(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  @Before
  public void setup() {
    msg = new TSimple();
    msg.setBln(true);
    msg.setBytes(ByteBuffer.wrap("bytes".getBytes(UTF_8)));
    msg.setDub(1.2d);
    msg.setShrt((short) 8);
    msg.setInteger(1);
    msg.setLng(Long.MAX_VALUE);
    msg.setStr("string");
    msg.setSingle_byte((byte) 1);

    mapping = new TSimpleMessageMapping();
  }

  @Test
  public void testFields() {
    List<Field> fields = mapping.getFields(msg);

    assertNotNull(fields);
    assertEquals(8, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("bln"), EMPTY, EMPTY_CV, value("true")));
    expectedFields.add(new FieldImpl(text("bytes"), EMPTY, EMPTY_CV, value("bytes")));
    expectedFields.add(new FieldImpl(text("dub"), EMPTY, EMPTY_CV, value("1.2")));
    expectedFields.add(new FieldImpl(text("shrt"), EMPTY, EMPTY_CV, value("8")));
    expectedFields.add(new FieldImpl(text("integer"), EMPTY, EMPTY_CV, value("1")));
    expectedFields.add(new FieldImpl(text("lng"), EMPTY, EMPTY_CV, value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("str"), EMPTY, EMPTY_CV, value("string")));
    expectedFields.add(new FieldImpl(text("single_byte"), EMPTY, EMPTY_CV, value("1")));

    assertTrue("Fields were not changed", fields.removeAll(expectedFields));
    assertTrue("Leftover fields not removed: " + fields, fields.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresInstace() {
    InstanceOrBuilder<TSimple> instOrBuilder = new InstanceOrBuilder<TSimple>() {
      @Override
      public cereal.InstanceOrBuilder.Type getType() {
        return Type.BUILDER;
      }

      @Override
      public Object get() {
        return null;
      }

      @Override
      public Class<TSimple> getWrappedClass() {
        return null;
      }
    };
    mapping.update(Collections.<Entry<Key,Value>> emptyList(), instOrBuilder);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessage() {
    mapping.getFields(null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullBuilder() {
    mapping.update(Collections.<Entry<Key,Value>> emptyList(), null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullEntry() {
    InstanceOrBuilder<TSimple> instOrBuilder = new InstanceOrBuilderImpl<>(msg);
    mapping.update(null, instOrBuilder);
  }

  @Test
  public void testUpdate() {
    TSimple newMsg = new TSimple();
    InstanceOrBuilder<TSimple> instOrBuilder = new InstanceOrBuilderImpl<>(newMsg);

    Map<Key,Value> data = new HashMap<>();
    data.put(new Key("id1", "", "bln"), value("true"));
    data.put(new Key("id1", "", "bytes"), value("bytes"));
    data.put(new Key("id1", "", "dub"), value("1.2"));
    data.put(new Key("id1", "", "shrt"), value("8"));
    data.put(new Key("id1", "", "integer"), value("1"));
    data.put(new Key("id1", "", "lng"), value(Long.toString(Long.MAX_VALUE)));
    data.put(new Key("id1", "", "str"), value("string"));
    data.put(new Key("id1", "", "single_byte"), value("1"));

    mapping.update(data.entrySet(), instOrBuilder);

    assertEquals(msg, newMsg);
  }

  @Test
  public void testIgnoredTypes() {
    TComplex complexMsg = new TComplex();
    complexMsg.setSimple(msg);
    complexMsg.addToStrings("string1");
    complexMsg.addToStrings("string2");

    TComplexMessageMapping complexMapping = new TComplexMessageMapping();

    // Serializing a message with a repeated field is just ignored
    List<Field> fields = complexMapping.getFields(complexMsg);
    assertNotNull(fields);
    assertEquals(0, fields.size());

    TComplex newComplexMsg = new TComplex();
    // Unable to deserialize some hypothetical key-value
    complexMapping.update(ImmutableMap.of(new Key("id1", "", "str_list"), value("string1,string2")).entrySet(), new InstanceOrBuilderImpl<TComplex>(
        newComplexMsg));

    assertEquals(0, newComplexMsg.getStringsSize());
    assertNull(newComplexMsg.getSimple());
  }

  @Test
  public void testGroupingAndVisibility() {
    TSpecialMessageMapping specialMapping = new TSpecialMessageMapping();
    List<Field> fields = specialMapping.getFields(msg);

    assertNotNull(fields);
    assertEquals(8, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("bln"), text("b"), visibility("l"), value("true")));
    expectedFields.add(new FieldImpl(text("bytes"), text("b"), visibility("y"), value("bytes")));
    expectedFields.add(new FieldImpl(text("dub"), text("d"), visibility("u"), value("1.2")));
    expectedFields.add(new FieldImpl(text("shrt"), text("s"), visibility("h"), value("8")));
    expectedFields.add(new FieldImpl(text("integer"), text("i"), visibility("n"), value("1")));
    expectedFields.add(new FieldImpl(text("lng"), text("l"), visibility("n"), value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("str"), text("s"), visibility("t"), value("string")));
    expectedFields.add(new FieldImpl(text("single_byte"), text("s"), visibility("i"), value("1")));

    assertTrue("Fields were not changed", fields.removeAll(expectedFields));
    assertTrue("Leftover fields not removed: " + fields, fields.isEmpty());
  }

}
