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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.InstanceOrBuilder.Type;
import cereal.Mapping;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * A generic {@link Mapping} for ProtocolBuffer {@link Message}s.
 */
public abstract class ProtobufMessageMapping<T extends GeneratedMessage> implements Mapping<T> {
  private static final Logger log = LoggerFactory.getLogger(ProtobufMessageMapping.class);
  private static final char PERIOD = '.';

  @Override
  public List<Field> getFields(T msg) {
    checkNotNull(msg, "Message was null");

    final Map<FieldDescriptor,Object> pbFields = msg.getAllFields();
    final List<Field> fields = new ArrayList<>(pbFields.size());

    for (Entry<FieldDescriptor,Object> entry : pbFields.entrySet()) {
      final FieldDescriptor descriptor = entry.getKey();
      final String fieldName = descriptor.getName();
      switch (descriptor.getJavaType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case STRING:
          if (descriptor.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<Object> objects = (List<Object>) entry.getValue();

            int repetition = 0;
            for (Object obj : objects) {
              fields.add(new FieldImpl(text(qualifyWithRepetition(fieldName, repetition)), getGrouping(descriptor), getVisibility(descriptor), value(obj
                  .toString())));
              repetition++;
            }
          } else {
            fields.add(new FieldImpl(text(fieldName), getGrouping(descriptor), getVisibility(descriptor), value(entry.getValue().toString())));
          }
          break;
        case BYTE_STRING:
          if (descriptor.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<ByteString> byteStrings = (List<ByteString>) entry.getValue();

            int repetition = 0;
            for (ByteString bs : byteStrings) {
              fields.add(new FieldImpl(text(qualifyWithRepetition(fieldName, repetition)), getGrouping(descriptor), getVisibility(descriptor), new Value(bs
                  .toByteArray())));
              repetition++;
            }
          } else {
            ByteString bs = (ByteString) entry.getValue();
            fields.add(new FieldImpl(text(fieldName), getGrouping(descriptor), getVisibility(descriptor), new Value(bs.toByteArray())));

          }
          break;
        default:
          log.warn("Ignoring complex field type: " + descriptor.getJavaType());
          break;
      }
    }

    return fields;
  }

  /**
   * The grouping for a field.
   *
   * @param field
   *          The protocol buffer field
   * @return The grouping for the field
   */
  public abstract Text getGrouping(FieldDescriptor field);

  /**
   * The visibility for a field.
   *
   * @param field
   *          The protocol buffer field.
   * @return The visibility for the field
   */
  public abstract ColumnVisibility getVisibility(FieldDescriptor field);

  private Text text(String str) {
    return new Text(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  private String qualifyWithRepetition(String fieldName, Integer repetition) {
    return fieldName + PERIOD + repetition;
  }

  @Override
  public void update(Entry<Key,Value> entry, InstanceOrBuilder<T> obj) {
    checkNotNull(entry, "Key-Value pair was null");
    checkNotNull(obj, "InstanceOrBuilder was null");
    checkArgument(Type.BUILDER == obj.getType(), "Expected argument to be a builder");

    final GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) obj.get();
    String fieldName = entry.getKey().getColumnQualifier().toString();

    // Find the FieldDescriptor from the Key
    for (FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
      if (fieldDesc.isRepeated()) {
        int offset = fieldName.lastIndexOf(PERIOD);
        fieldName = fieldName.substring(0, offset);
      }
      if (fieldName.equals(fieldDesc.getName())) {
        Value value = entry.getValue();
        switch (fieldDesc.getJavaType()) {
          case INT:
            Integer intVal = Integer.parseInt(value.toString());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, intVal);
            } else {
              builder.setField(fieldDesc, intVal);
            }
            break;
          case LONG:
            Long longVal = Long.parseLong(value.toString());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, longVal);
            } else {
              builder.setField(fieldDesc, longVal);
            }
            break;
          case FLOAT:
            Float floatVal = Float.parseFloat(value.toString());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, floatVal);
            } else {
              builder.setField(fieldDesc, floatVal);
            }
            break;
          case DOUBLE:
            Double doubleVal = Double.parseDouble(value.toString());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, doubleVal);
            } else {
              builder.setField(fieldDesc, doubleVal);
            }
            break;
          case BOOLEAN:
            Boolean booleanVal = Boolean.parseBoolean(value.toString());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, booleanVal);
            } else {
              builder.setField(fieldDesc, booleanVal);
            }
            break;
          case STRING:
            String strVal = value.toString();
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, strVal);
            } else {
              builder.setField(fieldDesc, strVal);
            }
            break;
          case BYTE_STRING:
            ByteString byteStrVal = ByteString.copyFrom(entry.getValue().get());
            if (fieldDesc.isRepeated()) {
              builder.addRepeatedField(fieldDesc, byteStrVal);
            } else {
              builder.setField(fieldDesc, byteStrVal);
            }
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
