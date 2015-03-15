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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.InstanceOrBuilder.Type;
import cereal.Mapping;
import cereal.Registry;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * A generic {@link Mapping} for ProtocolBuffer {@link Message}s.
 */
public abstract class ProtobufMessageMapping<T extends GeneratedMessage> implements Mapping<T> {
  private static final Logger log = LoggerFactory.getLogger(ProtobufMessageMapping.class);
  private static final char PERIOD = '.', DOLLAR = '$';
  private static final String EMPTY = "";

  private final Registry registry;

  public ProtobufMessageMapping(Registry registry) {
    this.registry = registry;
  }

  @Override
  public List<Field> getFields(T msg) {
    checkNotNull(msg, "Message was null");

    final List<Field> fields = new ArrayList<>(32);
    _getFields(msg, fields, EMPTY);

    return fields;
  }

  void _getFields(GeneratedMessage msg, List<Field> fields, String prefix) {
    final Map<FieldDescriptor,Object> pbFields = msg.getAllFields();

    for (Entry<FieldDescriptor,Object> entry : pbFields.entrySet()) {
      final FieldDescriptor descriptor = entry.getKey();
      final StringBuilder fieldName = new StringBuilder(32);
      fieldName.append(prefix).append(descriptor.getName());

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
              fields.add(new FieldImpl(text(qualifyWithRepetition(fieldName.toString(), repetition)), getGrouping(descriptor), getVisibility(descriptor),
                  value(obj.toString())));
              repetition++;
            }
          } else {
            fields.add(new FieldImpl(text(fieldName.toString()), getGrouping(descriptor), getVisibility(descriptor), value(entry.getValue().toString())));
          }
          break;
        case BYTE_STRING:
          if (descriptor.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<ByteString> byteStrings = (List<ByteString>) entry.getValue();

            int repetition = 0;
            for (ByteString bs : byteStrings) {
              fields.add(new FieldImpl(text(qualifyWithRepetition(fieldName.toString(), repetition)), getGrouping(descriptor), getVisibility(descriptor),
                  new Value(bs.toByteArray())));
              repetition++;
            }
          } else {
            ByteString bs = (ByteString) entry.getValue();
            fields.add(new FieldImpl(text(fieldName.toString()), getGrouping(descriptor), getVisibility(descriptor), new Value(bs.toByteArray())));
          }
          break;
        case MESSAGE:
          if (descriptor.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<GeneratedMessage> subMsgs = (List<GeneratedMessage>) entry.getValue();
            int repetition = 0;

            for (GeneratedMessage subMsg : subMsgs) {
              InstanceOrBuilder<GeneratedMessage> subIob = new InstanceOrBuilderImpl<>(subMsg);
              Mapping<GeneratedMessage> subMapping = registry.get(subIob);
              if (null == subMapping) {
                throw new RuntimeException("No Mapping in Registry for " + subMsg.getClass());
              }

              if (subMapping instanceof ProtobufMessageMapping) {
                String finalName = qualifyWithRepetition(fieldName.toString(), repetition) + PERIOD;
                ((ProtobufMessageMapping<?>) subMapping)._getFields(subMsg, fields, finalName);
              } else {
                throw new RuntimeException("Expected ProtobufMessageMapping but got " + subMapping.getClass());
              }

              repetition++;
            }
          } else {
            GeneratedMessage subMsg = (GeneratedMessage) entry.getValue();
            InstanceOrBuilder<GeneratedMessage> subIob = new InstanceOrBuilderImpl<>(subMsg);
            Mapping<GeneratedMessage> subMapping = registry.get(subIob);
            if (subMapping instanceof ProtobufMessageMapping) {
              fieldName.append(PERIOD);
              ((ProtobufMessageMapping<?>) subMapping)._getFields(subMsg, fields, fieldName.toString());
            } else {
              throw new RuntimeException("Expected ProtobufMessageMapping but got " + subMapping.getClass());
            }
          }

          break;
        default:
          log.warn("Ignoring complex field type: " + descriptor.getJavaType());
          break;
      }
    }
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
    return fieldName + DOLLAR + repetition;
  }

  @Override
  public void update(Iterable<Entry<Key,Value>> iter, InstanceOrBuilder<T> obj) {
    checkNotNull(iter, "Iterable was null");
    checkNotNull(obj, "InstanceOrBuilder was null");
    checkArgument(Type.BUILDER == obj.getType(), "Expected argument to be a builder");

    final GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) obj.get();
    final List<Entry<Key,Value>> leftoverFields = new LinkedList<>();

    for (Entry<Key,Value> entry : iter) {
      String fieldName = entry.getKey().getColumnQualifier().toString();

      int index = fieldName.indexOf(PERIOD);
      if (0 <= index) {
        leftoverFields.add(entry);
        continue;
      }

      // Find the FieldDescriptor from the Key
      for (FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
        if (fieldDesc.isRepeated()) {
          int offset = fieldName.lastIndexOf(DOLLAR);
          if (offset < 0) {
            throw new RuntimeException("Could not find offset of separator for repeated field count in " + fieldName);
          }
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
          break;
        }
      }
    }

    // All primitives in object should be filled out.
    // Make sure nested messages get filled out too.

    if (!leftoverFields.isEmpty()) {
      for (FieldDescriptor fieldDesc : builder.getDescriptorForType().getFields()) {
        if (JavaType.MESSAGE == fieldDesc.getJavaType()) {
          // For each Key-Value pair which have this prefix as the fieldname (column qualifier)
          final String fieldName = fieldDesc.getName();
          final String singularPrefix = fieldName + PERIOD, repeatedPrefix = fieldName + DOLLAR;

          log.debug("Extracting Key-Value pairs for {}", fieldDesc.getName());

          // Use a TreeMap to ensure the correct repetition order is preserved
          Map<Integer,List<Entry<Key,Value>>> fieldsForNestedMessage = new TreeMap<>();

          final Text _holder = new Text();
          Iterator<Entry<Key,Value>> leftoverFieldsIter = leftoverFields.iterator();
          while (leftoverFieldsIter.hasNext()) {
            final Entry<Key,Value> entry = leftoverFieldsIter.next();
            final Key key = entry.getKey();
            entry.getKey().getColumnQualifier(_holder);

            String colqual = _holder.toString();
            if (colqual.startsWith(singularPrefix)) {
              // Make a copy of the original Key, stripping the prefix off of the qualifier
              Key copy = new Key(key.getRow(), key.getColumnFamily(), new Text(colqual.substring(singularPrefix.length())), key.getColumnVisibility(),
                  key.getTimestamp());

              List<Entry<Key,Value>> kvPairs = fieldsForNestedMessage.get(-1);
              if (null == kvPairs) {
                kvPairs = new LinkedList<>();
                fieldsForNestedMessage.put(-1, kvPairs);
              }
              kvPairs.add(Maps.immutableEntry(copy, entry.getValue()));

              // Remove it from the list as we should never have to reread this one again
              leftoverFieldsIter.remove();
            } else if (colqual.startsWith(repeatedPrefix)) {
              // Make a copy of the original Key, stripping the prefix off of the qualifier
              int index = colqual.indexOf(PERIOD, repeatedPrefix.length());
              if (0 > index) {
                throw new RuntimeException("Could not find period after dollar sign: " + colqual);
              }

              Integer repetition = Integer.parseInt(colqual.substring(repeatedPrefix.length(), index));

              Key copy = new Key(key.getRow(), key.getColumnFamily(), new Text(colqual.substring(index + 1)), key.getColumnVisibility(),
                  key.getTimestamp());

              List<Entry<Key,Value>> kvPairs = fieldsForNestedMessage.get(repetition);
              if (null == kvPairs) {
                kvPairs = new LinkedList<>();
                fieldsForNestedMessage.put(repetition, kvPairs);
              }
              kvPairs.add(Maps.immutableEntry(copy, entry.getValue()));

              // Remove it from the list as we should never have to reread this one again
              leftoverFieldsIter.remove();
            }
          }

          if (!fieldsForNestedMessage.isEmpty()) {
            // We have keys, pass them down to the nested message
            String nestedMsgClzName = getClassName(fieldDesc);

            log.debug("Found {} Key-Value pairs for {}. Reconstituting the message.", fieldsForNestedMessage.size(), nestedMsgClzName);

            try {
              @SuppressWarnings("unchecked")
              // Get the class, builder and InstanceOrBuilder for the nested message
              Class<GeneratedMessage> msgClz = (Class<GeneratedMessage>) Class.forName(nestedMsgClzName);
              Method newBuilderMethod = msgClz.getMethod("newBuilder");

              for (Entry<Integer,List<Entry<Key,Value>>> pairsPerRepetition : fieldsForNestedMessage.entrySet()) {
                Message.Builder subBuilder = (Message.Builder) newBuilderMethod.invoke(null);
                InstanceOrBuilder<GeneratedMessage> subIob = new InstanceOrBuilderImpl<>(subBuilder, msgClz);

                // Get the mapping from the registry
                ProtobufMessageMapping<GeneratedMessage> subMapping = (ProtobufMessageMapping<GeneratedMessage>) registry.get(subIob);

                // Invoke update on the mapping with the subset of Key-Values
                subMapping.update(pairsPerRepetition.getValue(), subIob);

                // Set the result on the top-level obj
                if (fieldDesc.isRepeated()) {
                  builder.addRepeatedField(fieldDesc, subBuilder.build());
                } else {
                  builder.setField(fieldDesc, subBuilder.build());
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          // No fields for the sub message, therefore it's empty
          log.debug("Found no Key-Value pairs for {}", fieldName);
        }
        // Not a message, so we can ignore it
      }

      if (!leftoverFields.isEmpty()) {
        log.warn("Found {} leftover Key-Value pairs that were not consumed", leftoverFields.size());
      }
    }
  }

  protected String getClassName(FieldDescriptor fieldDesc) {
    FileDescriptor fileDesc = fieldDesc.getFile();
    FileOptions fileOptions = fileDesc.getOptions();

    String pkg;
    String baseJavaClassName;

    // Use the java package when present, the pb package when not.
    if (fileOptions.hasJavaPackage()) {
      pkg = fileOptions.getJavaPackage();
    } else {
      pkg = fileDesc.getPackage();
    }

    // Use the provided outer class name, or the pb file name
    if (fileOptions.hasJavaOuterClassname()) {
      baseJavaClassName = fileOptions.getJavaOuterClassname();
    } else {
      Iterable<String> pieces = Splitter.on('_').split(fileDesc.getName());
      StringBuilder sb = new StringBuilder(16);
      for (String piece : pieces) {
        if (!piece.isEmpty()) {
          sb.append(StringUtils.capitalize(piece));
        }
      }
      baseJavaClassName = sb.toString();
    }

    return pkg + "." + baseJavaClassName + "$" + fieldDesc.getMessageType().getName();
  }
}
