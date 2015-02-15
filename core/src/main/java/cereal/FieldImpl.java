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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import cereal.Field;

/**
 *
 */
public class FieldImpl implements Field {
  private static final Text EMPTY = new Text();
  private static final ColumnVisibility ALL = new ColumnVisibility();

  private Text name, grouping;
  private ColumnVisibility visibility;
  private Value value;

  public FieldImpl(Text name, Text grouping, ColumnVisibility visibility, Value value) {
    checkNotNull(name);
    checkNotNull(value);
    this.name = name;
    this.grouping = grouping;
    this.visibility = visibility;
    this.value = value;
  }

  @Override
  public Text name() {
    return name;
  }

  @Override
  public Text grouping() {
    if (null == grouping) {
      return EMPTY;
    }
    return grouping;
  }

  @Override
  public ColumnVisibility visibility() {
    if (null == visibility) {
      return ALL;
    }
    return visibility;
  }

  @Override
  public Value value() {
    return value;
  }

}
