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

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * An attribute on some simple object being serialized.
 */
public interface Field {

  /**
   * The name of this field. Identifies it among other fields in an object.
   *
   * @return The name of this field.
   */
  public Text name();

  /**
   * An optional grouping, a logical value to colocate this field with other fields.
   *
   * @return A grouping for this field, null if there is no grouping.
   */
  public Text grouping();

  /**
   * The optional {@link ColumnVisibility} for this field.
   *
   * @return The {@link ColumnVisibility} or null if there is no visibility defined.
   */
  public ColumnVisibility visibility();

  /**
   * The value of this field.
   * 
   * @return The value of this field.
   */
  public Value value();
}
