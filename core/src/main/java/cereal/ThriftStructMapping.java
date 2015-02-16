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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default {@link Mapping} implementation for Thrift structs.
 */
public abstract class ThriftStructMapping<E extends TBase<? extends TBase<?,?>,? extends TFieldIdEnum>> implements Mapping<E> {
  private static final Logger log = LoggerFactory.getLogger(ThriftStructMapping.class);

  private volatile Method getFieldValue, isSet, setFieldValue;

  @Override
  public List<Field> getFields(E obj) {
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
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(booleanVal.toString())));
              break;
            case TType.BYTE:
              Byte byteVal = (Byte) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(byteVal.toString())));
              break;
            case TType.DOUBLE:
              Double dblVal = (Double) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(dblVal.toString())));
              break;
            case TType.I16:
              Short shortVal = (Short) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(shortVal.toString())));
              break;
            case TType.I32:
              Integer intVal = (Integer) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(intVal.toString())));
              break;
            case TType.I64:
              Long longVal = (Long) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(longVal.toString())));
              break;
            case TType.STRING:
              String strVal = (String) value;
              fields.add(new FieldImpl(text(fMetaData.fieldName), null, null, value(strVal)));
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

  private Text text(String str) {
    return new Text(str);
  }

  private Value value(String str) {
    return new Value(str.getBytes(UTF_8));
  }

  @Override
  public void update(Entry<Key,Value> entry, E obj) {
    try {
      @SuppressWarnings("rawtypes")
      Class<? extends TBase> tbaseClz = obj.getClass();
      if (null == setFieldValue) {
        synchronized (this) {
          if (null == setFieldValue) {
            Class<?> fieldsClz = Class.forName(obj.getClass().getName() + "$_Fields");
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
              String strVal = v.toString();
              setFieldValue.invoke(obj, fieldId, strVal);
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
