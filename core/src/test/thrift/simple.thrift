namespace java cereal.objects.thrift

struct TSimple {
  1:string str
  8:i16 shrt
  2:i32 integer
  3:i64 lng
  4:binary bytes
  5:bool bln
  6:byte single_byte
  7:double dub
}

struct TComplex {
  1:TSimple simple
  2:list<string> strings
}
