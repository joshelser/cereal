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
package cereal.impl.objects.pojo;

/**
 * A simple POJO for testing stuff
 */
public class SimplePojo {

  short shrt;
  int integer;
  long lng;
  float flt;
  double dbl;
  String str;
  byte b;

  public SimplePojo() {}
  
  public SimplePojo(short shrt, int integer, long lng, float flt, double dbl, String str, byte b) {
    this.shrt = shrt;
    this.integer = integer;
    this.lng = lng;
    this.flt = flt;
    this.dbl = dbl;
    this.str = str;
    this.b = b;
  }

  /**
   * @return the shrt
   */
  public short getShrt() {
    return shrt;
  }

  /**
   * @param shrt
   *          the shrt to set
   */
  public void setShrt(short shrt) {
    this.shrt = shrt;
  }

  /**
   * @return the integer
   */
  public int getInteger() {
    return integer;
  }

  /**
   * @param integer
   *          the integer to set
   */
  public void setInteger(int integer) {
    this.integer = integer;
  }

  /**
   * @return the lng
   */
  public long getLng() {
    return lng;
  }

  /**
   * @param lng
   *          the lng to set
   */
  public void setLng(long lng) {
    this.lng = lng;
  }

  /**
   * @return the flt
   */
  public float getFlt() {
    return flt;
  }

  /**
   * @param flt
   *          the flt to set
   */
  public void setFlt(float flt) {
    this.flt = flt;
  }

  /**
   * @return the dbl
   */
  public double getDbl() {
    return dbl;
  }

  /**
   * @param dbl
   *          the dbl to set
   */
  public void setDbl(double dbl) {
    this.dbl = dbl;
  }

  /**
   * @return the str
   */
  public String getStr() {
    return str;
  }

  /**
   * @param str
   *          the str to set
   */
  public void setStr(String str) {
    this.str = str;
  }

  /**
   * @return the b
   */
  public byte getB() {
    return b;
  }

  /**
   * @param b
   *          the b to set
   */
  public void setB(byte b) {
    this.b = b;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + b;
    long temp;
    temp = Double.doubleToLongBits(dbl);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + Float.floatToIntBits(flt);
    result = prime * result + integer;
    result = prime * result + (int) (lng ^ (lng >>> 32));
    result = prime * result + shrt;
    result = prime * result + ((str == null) ? 0 : str.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SimplePojo)) {
      return false;
    }
    SimplePojo other = (SimplePojo) obj;
    if (b != other.b)
      return false;
    if (Double.doubleToLongBits(dbl) != Double.doubleToLongBits(other.dbl))
      return false;
    if (Float.floatToIntBits(flt) != Float.floatToIntBits(other.flt))
      return false;
    if (integer != other.integer)
      return false;
    if (lng != other.lng)
      return false;
    if (shrt != other.shrt)
      return false;
    if (str == null) {
      if (other.str != null)
        return false;
    } else if (!str.equals(other.str))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Simple [shrt=" + shrt + ", integer=" + integer + ", lng=" + lng + ", flt=" + flt + ", dbl=" + dbl + ", str=" + str + ", b=" + b + "]";
  }

}
