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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.impl.FieldImpl;
import cereal.impl.InstanceOrBuilderImpl;
import cereal.impl.ProtobufMessageMapping;
import cereal.impl.objects.protobuf.SimpleOuter.Complex;
import cereal.impl.objects.protobuf.SimpleOuter.Simple;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

public class ProtobufMessageMappingTest {

  private Simple msg;
  private SimpleMessageMapping mapping;

  private static class SimpleMessageMapping extends ProtobufMessageMapping<Simple> {
    @Override
    public Text getRowId(Simple obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<Simple> objectType() {
      return Simple.class;
    }
  }

  private static class ComplexMessageMapping extends ProtobufMessageMapping<Complex> {
    @Override
    public Text getRowId(Complex obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<Complex> objectType() {
      return Complex.class;
    }
  }

  private Text text(String str) {
    return new Text(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  @Before
  public void setup() {
    msg = Simple.newBuilder().setBoolean(true).setByteStr(ByteString.copyFromUtf8("bytestring")).setDub(1.2d).setFlt(2.1f).setInt(1).setLong(Long.MAX_VALUE)
        .setStr("string").build();
    mapping = new SimpleMessageMapping();
  }

  @Test
  public void testFields() {
    List<Field> fields = mapping.getFields(msg);
    assertNotNull(fields);
    assertEquals(7, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("boolean"), null, null, value("true")));
    expectedFields.add(new FieldImpl(text("byte_str"), null, null, value("bytestring")));
    expectedFields.add(new FieldImpl(text("dub"), null, null, value("1.2")));
    expectedFields.add(new FieldImpl(text("flt"), null, null, value("2.1")));
    expectedFields.add(new FieldImpl(text("int"), null, null, value("1")));
    expectedFields.add(new FieldImpl(text("long"), null, null, value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("str"), null, null, value("string")));

    assertTrue("Fields were not changed", fields.removeAll(expectedFields));
    assertTrue("Leftover fields not removed: " + fields, fields.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresBuilder() {
    InstanceOrBuilder<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(msg);
    mapping.update(Maps.immutableEntry(new Key(), new Value()), instOrBuilder);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMessage() {
    mapping.getFields(null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullBuilder() {
    mapping.update(Maps.immutableEntry(new Key(), new Value()), null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullEntry() {
    Simple.Builder builder = Simple.newBuilder();
    InstanceOrBuilder<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(builder, Simple.class);
    mapping.update(null, instOrBuilder);
  }

  @Test
  public void testUpdate() {
    Simple.Builder builder = Simple.newBuilder();
    InstanceOrBuilder<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(builder, Simple.class);

    Map<Key,Value> data = new HashMap<>();
    data.put(new Key("id1", "", "boolean"), value("true"));
    data.put(new Key("id1", "", "byte_str"), value("bytestring"));
    data.put(new Key("id1", "", "dub"), value("1.2"));
    data.put(new Key("id1", "", "flt"), value("2.1"));
    data.put(new Key("id1", "", "int"), value("1"));
    data.put(new Key("id1", "", "long"), value(Long.toString(Long.MAX_VALUE)));
    data.put(new Key("id1", "", "str"), value("string"));

    for (Entry<Key,Value> entry : data.entrySet()) {
      mapping.update(entry, instOrBuilder);
    }

    assertEquals(msg, builder.build());
  }

  @Test
  public void testIgnoredTypes() {
    Complex complexMsg = Complex.newBuilder().addStrList("string1").addStrList("value2").build();

    ComplexMessageMapping complexMapping = new ComplexMessageMapping();

    // Serializing a message with a repeated field is just ignored
    List<Field> fields = complexMapping.getFields(complexMsg);
    assertNotNull(fields);
    assertEquals(0, fields.size());

    Complex.Builder builder = Complex.newBuilder();
    // Unable to deserialize some hypothetical key-value
    complexMapping.update(Maps.immutableEntry(new Key("id1", "", "str_list"), value("string1,string2")), new InstanceOrBuilderImpl<Complex>(builder,
        Complex.class));

    Complex emptyMsg = builder.build();
    assertEquals(0, emptyMsg.getStrListCount());
  }
}
