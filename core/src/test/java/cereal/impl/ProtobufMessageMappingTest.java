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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.Registry;
import cereal.impl.objects.protobuf.SimpleOuter.Complex;
import cereal.impl.objects.protobuf.SimpleOuter.Nested;
import cereal.impl.objects.protobuf.SimpleOuter.Simple;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtobufMessageMappingTest {
  private static final Text EMPTY = new Text(new byte[0]);
  private static final ColumnVisibility EMPTY_CV = new ColumnVisibility("");

  private Simple msg;
  private SimpleMessageMapping mapping;
  private Registry registry;

  private static class SimpleMessageMapping extends ProtobufMessageMapping<Simple> {

    public SimpleMessageMapping(Registry reg) {
      super(reg);
    }

    @Override
    public Text getRowId(Simple obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<Simple> objectType() {
      return Simple.class;
    }

    @Override
    public Text getGrouping(FieldDescriptor field) {
      return EMPTY;
    }

    @Override
    public ColumnVisibility getVisibility(FieldDescriptor field) {
      return EMPTY_CV;
    }
  }

  private static class ComplexMessageMapping extends ProtobufMessageMapping<Complex> {

    public ComplexMessageMapping(Registry reg) {
      super(reg);
    }

    @Override
    public Text getRowId(Complex obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<Complex> objectType() {
      return Complex.class;
    }

    @Override
    public Text getGrouping(FieldDescriptor field) {
      return EMPTY;
    }

    @Override
    public ColumnVisibility getVisibility(FieldDescriptor field) {
      return EMPTY_CV;
    }
  }

  private static class NestedMessageMapping extends ProtobufMessageMapping<Nested> {
    public NestedMessageMapping(Registry reg) {
      super(reg);
    }

    @Override
    public Text getRowId(Nested obj) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Text getGrouping(FieldDescriptor field) {
      return EMPTY;
    }

    @Override
    public ColumnVisibility getVisibility(FieldDescriptor field) {
      return EMPTY_CV;
    }

    @Override
    public Class<Nested> objectType() {
      return Nested.class;
    }
  }

  private static class SpecialMessageMapping extends SimpleMessageMapping {

    public SpecialMessageMapping(Registry reg) {
      super(reg);
    }

    @Override
    public Text getGrouping(FieldDescriptor field) {
      // Grouping is the field character of the field name
      return new Text(field.getName().substring(0, 1));
    }

    @Override
    public ColumnVisibility getVisibility(FieldDescriptor field) {
      // Visibility is the java type name
      return new ColumnVisibility(field.getJavaType().toString());
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
    msg = Simple.newBuilder().setBoolean(true).setByteStr(ByteString.copyFromUtf8("bytestring")).setDub(1.2d).setFlt(2.1f).setInt(1).setLong(Long.MAX_VALUE)
        .setStr("string").build();
    registry = new RegistryImpl();
    mapping = new SimpleMessageMapping(registry);
  }

  @Test
  public void testFields() {
    List<Field> fields = mapping.getFields(msg);
    assertNotNull(fields);
    assertEquals(7, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("boolean"), EMPTY, EMPTY_CV, value("true")));
    expectedFields.add(new FieldImpl(text("byte_str"), EMPTY, EMPTY_CV, value("bytestring")));
    expectedFields.add(new FieldImpl(text("dub"), EMPTY, EMPTY_CV, value("1.2")));
    expectedFields.add(new FieldImpl(text("flt"), EMPTY, EMPTY_CV, value("2.1")));
    expectedFields.add(new FieldImpl(text("int"), EMPTY, EMPTY_CV, value("1")));
    expectedFields.add(new FieldImpl(text("long"), EMPTY, EMPTY_CV, value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("str"), EMPTY, EMPTY_CV, value("string")));

    assertTrue("Fields were not changed", fields.removeAll(expectedFields));
    assertTrue("Leftover fields not removed: " + fields, fields.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequiresBuilder() {
    InstanceOrBuilder<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(msg);
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

    mapping.update(data.entrySet(), instOrBuilder);

    assertEquals(msg, builder.build());
  }

  @Test
  public void testRepeatedFields() {
    Complex complexMsg = Complex.newBuilder().addStrList("string1").addStrList("string2").build();

    ComplexMessageMapping complexMapping = new ComplexMessageMapping(registry);

    // Serializing a message with a repeated field is just ignored
    List<Field> fields = complexMapping.getFields(complexMsg);
    assertNotNull(fields);
    assertEquals(2, fields.size());

    List<Field> expectedFields = new ArrayList<>();
    expectedFields.add(new FieldImpl(text("str_list$0"), EMPTY, EMPTY_CV, value("string1")));
    expectedFields.add(new FieldImpl(text("str_list$1"), EMPTY, EMPTY_CV, value("string2")));

    expectedFields.removeAll(fields);
    assertTrue("Fields unexpectedly not generated by mapping: " + expectedFields, expectedFields.isEmpty());

    Complex.Builder builder = Complex.newBuilder();
    InstanceOrBuilder<Complex> iob = new InstanceOrBuilderImpl<Complex>(builder, Complex.class);
    Map<Key,Value> data = ImmutableMap.of(new Key("id1", "", "str_list$0"), value("string1"), new Key("id1", "", "str_list$1"), value("string2"));
    complexMapping.update(data.entrySet(), iob);

    Complex copyMsg = builder.build();
    assertEquals(2, copyMsg.getStrListCount());
    assertEquals(complexMsg, copyMsg);
  }

  @Test
  public void testGroupingAndVisibility() {
    SpecialMessageMapping specialMapping = new SpecialMessageMapping(registry);
    List<Field> fields = specialMapping.getFields(msg);
    assertNotNull(fields);
    assertEquals(7, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("boolean"), text("b"), visibility("BOOLEAN"), value("true")));
    expectedFields.add(new FieldImpl(text("byte_str"), text("b"), visibility("BYTE_STRING"), value("bytestring")));
    expectedFields.add(new FieldImpl(text("dub"), text("d"), visibility("DOUBLE"), value("1.2")));
    expectedFields.add(new FieldImpl(text("flt"), text("f"), visibility("FLOAT"), value("2.1")));
    expectedFields.add(new FieldImpl(text("int"), text("i"), visibility("INT"), value("1")));
    expectedFields.add(new FieldImpl(text("long"), text("l"), visibility("LONG"), value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("str"), text("s"), visibility("STRING"), value("string")));

    assertTrue("Fields were not changed", fields.removeAll(expectedFields));
    assertTrue("Leftover fields not removed: " + fields, fields.isEmpty());
  }

  @Test
  public void testNestedMapping() {
    NestedMessageMapping nestedMapping = new NestedMessageMapping(registry);
    ComplexMessageMapping complexMapping = new ComplexMessageMapping(registry);

    registry.add(mapping);
    registry.add(complexMapping);
    registry.add(nestedMapping);

    Complex complexMsg = Complex.newBuilder().addStrList("string1").addStrList("string2").build();
    Nested nestedMsg = Nested.newBuilder().setComplex(complexMsg).setSimple(msg).build();

    List<Field> fields = nestedMapping.getFields(nestedMsg);

    assertEquals(9, fields.size());

    List<Field> expectedFields = new ArrayList<>(fields.size());
    expectedFields.add(new FieldImpl(text("simple.boolean"), EMPTY, EMPTY_CV, value("true")));
    expectedFields.add(new FieldImpl(text("simple.byte_str"), EMPTY, EMPTY_CV, value("bytestring")));
    expectedFields.add(new FieldImpl(text("simple.dub"), EMPTY, EMPTY_CV, value("1.2")));
    expectedFields.add(new FieldImpl(text("simple.flt"), EMPTY, EMPTY_CV, value("2.1")));
    expectedFields.add(new FieldImpl(text("simple.int"), EMPTY, EMPTY_CV, value("1")));
    expectedFields.add(new FieldImpl(text("simple.long"), EMPTY, EMPTY_CV, value(Long.toString(Long.MAX_VALUE))));
    expectedFields.add(new FieldImpl(text("simple.str"), EMPTY, EMPTY_CV, value("string")));
    expectedFields.add(new FieldImpl(text("complex.str_list$0"), EMPTY, EMPTY_CV, value("string1")));
    expectedFields.add(new FieldImpl(text("complex.str_list$1"), EMPTY, EMPTY_CV, value("string2")));

    expectedFields.removeAll(fields);

    assertTrue("Unexpected leftover fields: " + expectedFields, expectedFields.isEmpty());
  }
}
