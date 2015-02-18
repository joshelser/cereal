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
package cereal.pojo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import cereal.Field;
import cereal.FieldImpl;
import cereal.InstanceOrBuilder;
import cereal.Mapping;

public class PersonMapping implements Mapping<Person> {

  @Override
  public Text getRowId(Person obj) {
    String firstName = obj.getFirstName(), middleName = obj.getMiddleName(), lastName = obj.getLastName();
    StringBuilder row = new StringBuilder(32);
    if (null != firstName) {
      row.append(firstName);
    }
    if (null != middleName) {
      if (row.length() > 0) {
        row.append("_");
      }
      row.append(middleName);
    }
    if (null != lastName) {
      if (row.length() > 0) {
        row.append("_");
      }
      row.append(lastName);
    }
    return new Text(row.toString());
  }

  @Override
  public List<Field> getFields(Person obj) {
    ArrayList<Field> fields = new ArrayList<>(6);
    if (null != obj.getFirstName()) {
      fields.add(new FieldImpl(text("first_name"), null, null, value(obj.getFirstName())));
    }
    if (null != obj.getMiddleName()) {
      fields.add(new FieldImpl(text("middle_name"), null, null, value(obj.getMiddleName())));
    }
    if (null != obj.getLastName()) {
      fields.add(new FieldImpl(text("last_name"), null, null, value(obj.getLastName())));
    }
    if (null != obj.getAge()) {
      fields.add(new FieldImpl(text("age"), null, null, value(Integer.toString(obj.getAge()))));
    }
    if (null != obj.getHeight()) {
      fields.add(new FieldImpl(text("height"), null, null, value(Integer.toString(obj.getHeight()))));
    }
    if (null != obj.getWeight()) {
      fields.add(new FieldImpl(text("weight"), null, null, value(Integer.toString(obj.getWeight()))));
    }
    return fields;
  }

  Text text(String txt) {
    return new Text(txt);
  }

  Value value(String val) {
    return new Value(val.getBytes(UTF_8));
  }

  @Override
  public void update(Entry<Key,Value> entry, InstanceOrBuilder<Person> instOrBuilder) {
    Person obj = (Person) instOrBuilder.get();
    Key k = entry.getKey();
    String colq = k.getColumnQualifier().toString();
    switch (colq) {
      case "first_name":
        obj.setFirstName(entry.getValue().toString());
        break;
      case "middle_name":
        obj.setMiddleName(entry.getValue().toString());
        break;
      case "last_name":
        obj.setLastName(entry.getValue().toString());
        break;
      case "age":
        obj.setAge(Integer.parseInt(entry.getValue().toString()));
        break;
      case "height":
        obj.setHeight(Integer.parseInt(entry.getValue().toString()));
        break;
      case "weight":
        obj.setWeight(Integer.parseInt(entry.getValue().toString()));
        break;
    }
  }

  @Override
  public Class<Person> objectType() {
    return Person.class;
  }

}
