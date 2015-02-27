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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import cereal.Field;

/**
 * Implementation of {@link Field}.
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("FieldImpl[name=").append(name).append(", grouping=").append(null != grouping ? grouping : "").append(", visibility=")
        .append(null != visibility ? visibility : "").append(", value=").append(value).append("]");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime * 1 + name.hashCode();
    result = prime * result + ((grouping == null) ? 0 : grouping.hashCode());
    result = prime * result + ((visibility == null) ? 0 : visibility.hashCode());
    result = prime * result + value.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof FieldImpl) {
      FieldImpl other = (FieldImpl) o;

      if (!name.equals(other.name)) {
        return false;
      }

      if (null == grouping) {
        if (null != other.grouping) {
          return false;
        }
      } else if (!grouping.equals(other.grouping)) {
        return false;
      }

      if (null == visibility) {
        if (null != visibility) {
          return false;
        }
      } else if (!visibility.equals(other.visibility)) {
        return false;
      }

      if (!value.equals(other.value)) {
        return false;
      }

      return true;
    }

    return false;
  }
}
