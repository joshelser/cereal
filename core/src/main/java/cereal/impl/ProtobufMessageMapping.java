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

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.Mapping;
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
  public List<Field> getFields(T msg) {
    checkNotNull(msg, "Message was null");

    final Map<FieldDescriptor,Object> pbFields = msg.getAllFields();
    final List<Field> fields = new ArrayList<>(pbFields.size());

    for (Entry<FieldDescriptor,Object> entry : pbFields.entrySet()) {
      FieldDescriptor descriptor = entry.getKey();
      // TODO implement repeated fields
      if (descriptor.isRepeated()) {
        continue;
      }
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
    checkNotNull(entry, "Key-Value pair was null");
    checkNotNull(obj, "InstanceOrBuilder was null");
    checkArgument(Type.BUILDER == obj.getType(), "Expected argument to be a builder");

    final GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) obj.get();
    final String fieldName = entry.getKey().getColumnQualifier().toString();

    // Find the FieldDescriptor from the Key
    for (FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
      // TODO implement repeated fields
      if (fieldDesc.isRepeated()) {
        continue;
      }
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
