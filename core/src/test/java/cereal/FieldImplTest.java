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
package cereal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class FieldImplTest {

  @Test
  public void optionalGroupingAndVisibility() {
    Text name = new Text("foo");
    Value value = new Value("bar".getBytes(UTF_8));
    FieldImpl field = new FieldImpl(name, null, null, value);
    assertEquals(name, field.name());
    assertEquals(value, field.value());
    
    assertEquals(new Text(""), field.grouping());
    assertEquals(new ColumnVisibility(""), field.visibility());
  }

  @Test
  public void grouping() {
    Text name = new Text("foo"), grouping = new Text("group1");
    Value value = new Value("bar".getBytes(UTF_8));
    FieldImpl field = new FieldImpl(name, grouping, null, value);
    assertEquals(name, field.name());
    assertEquals(grouping, field.grouping());
    assertEquals(value, field.value());
  }

  @Test
  public void visibility() {
    Text name = new Text("foo");
    ColumnVisibility cv = new ColumnVisibility("vis");
    Value value = new Value("bar".getBytes(UTF_8));
    FieldImpl field = new FieldImpl(name, null, cv, value);
    assertEquals(name, field.name());
    assertEquals(cv, field.visibility());
    assertEquals(value, field.value());
  }

  @Test
  public void groupingAndVisibility() {
    Text name = new Text("foo"), grouping = new Text("group1");
    ColumnVisibility cv = new ColumnVisibility("vis");
    Value value = new Value("bar".getBytes(UTF_8));
    FieldImpl field = new FieldImpl(name, grouping, cv, value);
    assertEquals(name, field.name());
    assertEquals(grouping, field.grouping());
    assertEquals(cv, field.visibility());
    assertEquals(value, field.value());
  }

  @Test
  public void equality() {
    Text name1 = new Text("foo");
    Value value1 = new Value("bar1".getBytes(UTF_8));
    FieldImpl field1 = new FieldImpl(name1, null, null, value1), copy = new FieldImpl(name1, null, null, value1);

    assertEquals(field1, copy);
    assertEquals(field1.hashCode(), copy.hashCode());

    Text name2 = new Text("foo1");

    FieldImpl field2 = new FieldImpl(name2, null, null, value1);

    assertNotEquals(field1, field2);
    assertNotEquals(field1.hashCode(), field2.hashCode());
  }

  @Test
  public void equalityGrouping() {
    Text name1 = new Text("foo"), grouping = new Text("group1");
    Value value1 = new Value("bar1".getBytes(UTF_8));
    FieldImpl field1 = new FieldImpl(name1, grouping, null, value1), copy = new FieldImpl(name1, grouping, null, value1);

    assertEquals(field1, copy);
    assertEquals(field1.hashCode(), copy.hashCode());

    Text grouping2 = new Text("group2");

    FieldImpl field2 = new FieldImpl(name1, grouping2, null, value1);

    assertNotEquals(field1, field2);
    assertNotEquals(field1.hashCode(), field2.hashCode());
  }

  @Test
  public void equalityGroupingVisibility() {
    Text name1 = new Text("foo"), grouping = new Text("group1");
    ColumnVisibility cv = new ColumnVisibility("vis1");
    Value value1 = new Value("bar1".getBytes(UTF_8));
    FieldImpl field1 = new FieldImpl(name1, grouping, cv, value1), copy = new FieldImpl(name1, grouping, cv, value1);

    assertEquals(field1, copy);
    assertEquals(field1.hashCode(), copy.hashCode());

    ColumnVisibility cv2 = new ColumnVisibility("vis1&vis2");

    FieldImpl field2 = new FieldImpl(name1, grouping, cv2, value1);

    assertNotEquals(field1, field2);
    assertNotEquals(field1.hashCode(), field2.hashCode());
  }

}
