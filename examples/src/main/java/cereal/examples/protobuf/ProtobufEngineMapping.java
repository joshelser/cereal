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

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import cereal.Registry;
import cereal.Serialization;
import cereal.examples.protobuf.generated.PersonOuter.Engine;
import cereal.impl.ProtobufMessageMapping;

import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtobufEngineMapping extends ProtobufMessageMapping<Engine> {
  private static final Text EMPTY = new Text(new byte[0]);
  private static final ColumnVisibility EMPTY_CV = new ColumnVisibility("");

  public ProtobufEngineMapping(Registry registry, Serialization serialization) {
    super(registry, serialization);
  }

  @Override
  public Text getRowId(Engine obj) {
    StringBuilder sb = new StringBuilder(32);
    if (obj.hasCylinders()) {
      sb.append(obj.getCylinders());
    }
    if (obj.hasDisplacement()) {
      if (0 < sb.length()) {
        sb.append("_");
      }
      sb.append(obj.getDisplacement());
    }
    if (obj.hasHorsepower()) {
      if (0 < sb.length()) {
        sb.append("_");
      }
      sb.append(obj.getHorsepower());
    }
    if (obj.hasTorque()) {
      if (0 < sb.length()) {
        sb.append("_");
      }
      sb.append(obj.getTorque());
    }
    return new Text(sb.toString());
  }

  @Override
  public Class<Engine> objectType() {
    return Engine.class;
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
