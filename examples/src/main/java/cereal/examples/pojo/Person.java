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
package cereal.examples.pojo;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Person {

  String firstName, middleName, lastName;
  Integer age, height, weight;

  /**
   * @return the firstName
   */
  public String getFirstName() {
    return firstName;
  }

  /**
   * @param firstName
   *          the firstName to set
   */
  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  /**
   * @return the middleName
   */
  public String getMiddleName() {
    return middleName;
  }

  /**
   * @param middleName
   *          the middleName to set
   */
  public void setMiddleName(String middleName) {
    this.middleName = middleName;
  }

  /**
   * @return the lastName
   */
  public String getLastName() {
    return lastName;
  }

  /**
   * @param lastName
   *          the lastName to set
   */
  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  /**
   * @return the age
   */
  public Integer getAge() {
    return age;
  }

  /**
   * @param age
   *          the age to set
   */
  public void setAge(Integer age) {
    this.age = age;
  }

  /**
   * @return the height
   */
  public Integer getHeight() {
    return height;
  }

  /**
   * @param height
   *          the height to set
   */
  public void setHeight(Integer height) {
    this.height = height;
  }

  /**
   * @return the weight
   */
  public Integer getWeight() {
    return weight;
  }

  /**
   * @param weight
   *          the weight to set
   */
  public void setWeight(Integer weight) {
    this.weight = weight;
  }

  @Override
  public String toString() {
    ToStringBuilder sb = new ToStringBuilder(this);
    sb.append("first name", firstName).append("middle name", middleName).append("last name", lastName);
    sb.append("age", age).append("weight", weight).append("height", height);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(19, 23);
    hcb.append(firstName).append(middleName).append(lastName).append(age).append(height).append(weight);
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Person) {
      Person other = (Person) o;
      if (null == firstName) {
        if (null != other.getFirstName()) {
          return false;
        }
      } else if (!firstName.equals(other.getFirstName())) {
        return false;
      }

      if (null == middleName) {
        if (null != other.getMiddleName()) {
          return false;
        }
      } else if (!middleName.equals(other.getMiddleName())) {
        return false;
      }

      if (null == lastName) {
        if (null != other.getLastName()) {
          return false;
        }
      } else if (!lastName.equals(other.getLastName())) {
        return false;
      }

      if (null == age) {
        if (null != other.getAge()) {
          return false;
        }
      } else if (!age.equals(other.getAge())) {
        return false;
      }

      if (null == height) {
        if (null != other.getHeight()) {
          return false;
        }
      } else if (!height.equals(other.getHeight())) {
        return false;
      }

      if (null == weight) {
        if (null != other.getWeight()) {
          return false;
        }
      } else if (!weight.equals(other.getWeight())) {
        return false;
      }
    }

    return false;

  }
}
