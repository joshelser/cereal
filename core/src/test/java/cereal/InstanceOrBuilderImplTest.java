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
package cereal;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cereal.InstanceOrBuilder.Type;
import cereal.objects.pojo.SimplePojo;
import cereal.objects.protobuf.SimpleOuter.Simple;
import cereal.objects.thrift.TSimple;

import com.google.protobuf.ByteString;

public class InstanceOrBuilderImplTest {

  @Test
  public void testThrift() {
    TSimple simple = new TSimple();
    InstanceOrBuilderImpl<TSimple> instOrBuilder = new InstanceOrBuilderImpl<>(simple);

    assertEquals(simple, instOrBuilder.get());
    assertEquals(TSimple.class, instOrBuilder.getWrappedClass());
    assertEquals(Type.INSTANCE, instOrBuilder.getType());
  }

  @Test
  public void testPojo() {
    SimplePojo pojo = new SimplePojo();
    pojo.setB((byte) 4);
    pojo.setInteger(1);
    pojo.setLng(21L);
    InstanceOrBuilderImpl<SimplePojo> instOrBuilder = new InstanceOrBuilderImpl<>(pojo);

    assertEquals(pojo, instOrBuilder.get());
    assertEquals(SimplePojo.class, instOrBuilder.getWrappedClass());
    assertEquals(Type.INSTANCE, instOrBuilder.getType());
  }

  @Test
  public void testProtoMsg() {
    Simple msg = Simple.newBuilder().setBoolean(true).setByteStr(ByteString.copyFromUtf8("bytes")).setDub(1d).setFlt(1f).setInt(1).setLong(1L).setStr("string")
        .build();
    InstanceOrBuilderImpl<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(msg);

    assertEquals(msg, instOrBuilder.get());
    assertEquals(Simple.class, instOrBuilder.getWrappedClass());
    assertEquals(Type.INSTANCE, instOrBuilder.getType());
  }

  @Test
  public void testProtoBuilder() {
    Simple.Builder msgBuilder = Simple.newBuilder();
    InstanceOrBuilderImpl<Simple> instOrBuilder = new InstanceOrBuilderImpl<>(msgBuilder, Simple.class);

    assertEquals(msgBuilder, instOrBuilder.get());
    assertEquals(Simple.class, instOrBuilder.getWrappedClass());
    assertEquals(Type.BUILDER, instOrBuilder.getType());
  }

}
