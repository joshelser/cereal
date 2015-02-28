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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cereal.Field;
import cereal.InstanceOrBuilder;
import cereal.InstanceOrBuilder.Type;
import cereal.Mapping;

/**
 * A default {@link Mapping} implementation for Thrift structs.
 */
public abstract class ThriftStructMapping<E extends TBase<? extends TBase<?,?>,? extends TFieldIdEnum>> implements Mapping<E> {
  private static final Logger log = LoggerFactory.getLogger(ThriftStructMapping.class);

  private volatile Method getFieldValue, isSet, setFieldValue;

  @Override
  public List<Field> getFields(E obj) {
    checkNotNull(obj, "The struct was null");

    List<Field> fields;
    try {
      @SuppressWarnings("rawtypes")
      Class<? extends TBase> tbaseClz = obj.getClass();
      if (null == getFieldValue) {
        synchronized (this) {
          if (null == getFieldValue) {
            Class<?> fieldsClz = Class.forName(obj.getClass().getName() + "$_Fields");
            getFieldValue = tbaseClz.getMethod("getFieldValue", fieldsClz);
            isSet = tbaseClz.getMethod("isSet", fieldsClz);
          }
        }
      }

      Map<? extends TFieldIdEnum,FieldMetaData> thriftFields = FieldMetaData.getStructMetaDataMap(tbaseClz);
      fields = new ArrayList<>();

      for (Entry<? extends TFieldIdEnum,FieldMetaData> entry : thriftFields.entrySet()) {
        TFieldIdEnum field = entry.getKey();
        FieldMetaData fMetaData = entry.getValue();
        if ((boolean) isSet.invoke(obj, field)) {
          Object value = getFieldValue.invoke(obj, field);
          FieldValueMetaData fvMetaData = fMetaData.valueMetaData;
          switch (fvMetaData.type) {
            case TType.BOOL:
              Boolean booleanVal = (Boolean) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(booleanVal.toString())));
              break;
            case TType.BYTE:
              Byte byteVal = (Byte) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(byteVal.toString())));
              break;
            case TType.DOUBLE:
              Double dblVal = (Double) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(dblVal.toString())));
              break;
            case TType.I16:
              Short shortVal = (Short) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(shortVal.toString())));
              break;
            case TType.I32:
              Integer intVal = (Integer) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(intVal.toString())));
              break;
            case TType.I64:
              Long longVal = (Long) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), value(longVal.toString())));
              break;
            case TType.STRING:
              byte[] bytes;
              if (fvMetaData.isBinary()) {
                bytes = (byte[]) value;
              } else {
                String strVal = (String) value;
                bytes = strVal.getBytes(UTF_8);
              }
              fields.add(new FieldImpl(text(fMetaData.fieldName), getGrouping(fMetaData), getVisibility(fMetaData), new Value(bytes)));
              break;
            default:
              log.warn("Ignoring field: {}", field.getFieldName());
              break;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
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
  public abstract Text getGrouping(FieldMetaData field);

  /**
   * The visibility for a field.
   *
   * @param field
   *          The protocol buffer field.
   * @return The visibility for the field
   */
  public abstract ColumnVisibility getVisibility(FieldMetaData field);

  private Text text(String str) {
    return new Text(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  @Override
  public void update(Entry<Key,Value> entry, InstanceOrBuilder<E> instOrBuilder) {
    checkNotNull(entry, "Key-Value pair is null");
    checkNotNull(instOrBuilder, "InstOrBuilder is null");
    checkArgument(Type.INSTANCE == instOrBuilder.getType(), "Expected INSTANCE in InstanceOrBuilder");

    try {
      @SuppressWarnings("rawtypes")
      Class<? extends TBase> tbaseClz = instOrBuilder.getWrappedClass();
      if (null == setFieldValue) {
        synchronized (this) {
          if (null == setFieldValue) {
            Class<?> fieldsClz = Class.forName(instOrBuilder.getWrappedClass().getName() + "$_Fields");
            setFieldValue = tbaseClz.getMethod("setFieldValue", fieldsClz, Object.class);
          }
        }
      }

      String fieldName = entry.getKey().getColumnQualifier().toString();
      Map<? extends TFieldIdEnum,FieldMetaData> thriftFields = FieldMetaData.getStructMetaDataMap(tbaseClz);
      for (Entry<? extends TFieldIdEnum,FieldMetaData> fieldEntry : thriftFields.entrySet()) {
        TFieldIdEnum fieldId = fieldEntry.getKey();
        if (fieldName.equals(fieldId.getFieldName())) {
          FieldValueMetaData fvMetaData = fieldEntry.getValue().valueMetaData;
          Value v = entry.getValue();
          Object obj = instOrBuilder.get();
          switch (fvMetaData.type) {
            case TType.BOOL:
              Boolean booleanVal = Boolean.parseBoolean(v.toString());
              setFieldValue.invoke(obj, fieldId, booleanVal);
              break;
            case TType.BYTE:
              Byte byteVal = Byte.parseByte(v.toString());
              setFieldValue.invoke(obj, fieldId, byteVal);
              break;
            case TType.DOUBLE:
              Double dblVal = Double.parseDouble(v.toString());
              setFieldValue.invoke(obj, fieldId, dblVal);
              break;
            case TType.I16:
              Short shortVal = Short.parseShort(v.toString());
              setFieldValue.invoke(obj, fieldId, shortVal);
              break;
            case TType.I32:
              Integer intVal = Integer.parseInt(v.toString());
              setFieldValue.invoke(obj, fieldId, intVal);
              break;
            case TType.I64:
              Long longVal = Long.parseLong(v.toString());
              setFieldValue.invoke(obj, fieldId, longVal);
              break;
            case TType.STRING:
              if (fvMetaData.isBinary()) {
                setFieldValue.invoke(obj, fieldId, ByteBuffer.wrap(v.get()));
              } else {
                String strVal = v.toString();
                setFieldValue.invoke(obj, fieldId, strVal);
              }
              break;
            default:
              log.warn("Ignoring field: {}", fieldName);
              break;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
