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
package cereal.examples.protobuf;

import org.apache.hadoop.io.Text;

import cereal.ProtobufMessageMapping;
import cereal.examples.protobuf.generated.PersonOuter.Person;

public class PersonMapping extends ProtobufMessageMapping<Person> {

  @Override
  public Text getRowId(Person obj) {
    StringBuilder sb = new StringBuilder(32);
    if (obj.hasFirstName()) {
      sb.append(obj.getFirstName());
    }
    if (obj.hasMiddleName()) {
      if (0 < sb.length()) {
        sb.append("_");
      }
      sb.append(obj.getMiddleName());
    }
    if (obj.hasLastName()) {
      if (0 < sb.length()) {
        sb.append("_");
      }
      sb.append(obj.getLastName());
    }
    return new Text(sb.toString());
  }

  @Override
  public Class<Person> objectType() {
    return Person.class;
  }

}
