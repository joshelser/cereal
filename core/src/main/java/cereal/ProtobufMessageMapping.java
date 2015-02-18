/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cereal.InstanceOrBuilder.Type;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * A generic {@link Mapping} for ProtocolBuffer {@link Message}s.
 */
public abstract class ProtobufMessageMapping<T extends GeneratedMessage> implements Mapping<T> {
  private static final Logger log = LoggerFactory.getLogger(ProtobufMessageMapping.class);

  @Override
  public List<Field> getFields(T obj) {
    checkNotNull(obj);

    final Map<FieldDescriptor,Object> pbFields = obj.getAllFields();
    final List<Field> fields = new ArrayList<>(pbFields.size());

    for (Entry<FieldDescriptor,Object> entry : pbFields.entrySet()) {
      FieldDescriptor descriptor = entry.getKey();
      switch (descriptor.getJavaType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case STRING:
          fields.add(new FieldImpl(text(descriptor.getName()), null, null, value(entry.getValue().toString())));
          break;
        case BYTE_STRING:
          ByteString bs = (ByteString) entry.getValue();
          fields.add(new FieldImpl(text(descriptor.getName()), null, null, new Value(bs.toByteArray())));
          break;
        default:
          log.warn("Ignoring complex field type: " + descriptor.getJavaType());
          break;
      }
    }

    return fields;
  }

  private Text text(String str) {
    return new Text(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  @Override
  public void update(Entry<Key,Value> entry, InstanceOrBuilder<T> obj) {
    checkArgument(Type.BUILDER == obj.getType(), "Expected argument to be a builder");

    final GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) obj.get();
    final String fieldName = entry.getKey().getColumnQualifier().toString();

    // Find the FieldDescriptor from the Key
    for (FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
      if (fieldName.equals(fieldDesc.getName())) {
        Value value = entry.getValue();
        switch (fieldDesc.getJavaType()) {
          case INT:
            builder.setField(fieldDesc, Integer.parseInt(value.toString()));
            break;
          case LONG:
            builder.setField(fieldDesc, Long.parseLong(value.toString()));
            break;
          case FLOAT:
            builder.setField(fieldDesc, Float.parseFloat(value.toString()));
            break;
          case DOUBLE:
            builder.setField(fieldDesc, Double.parseDouble(value.toString()));
            break;
          case BOOLEAN:
            builder.setField(fieldDesc, Boolean.parseBoolean(value.toString()));
            break;
          case STRING:
            builder.setField(fieldDesc, value.toString());
            break;
          case BYTE_STRING:
            byte[] bytes = entry.getValue().get();
            builder.setField(fieldDesc, ByteString.copyFrom(bytes));
            break;
          default:
            log.warn("Ignoring unknown serialized type {}", fieldDesc.getJavaType());
            break;
        }
        return;
      }
    }
  }
}
