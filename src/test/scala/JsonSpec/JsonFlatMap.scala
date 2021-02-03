package JsonSpec

import org.scalatest.wordspec.AnyWordSpecLike
import utility.JsonFlatMapper

class JsonFlatMap extends AnyWordSpecLike{

  "The flat map" should{
    "give 3 keys" in{
      val json="{\"A1\":10,\"A2\":{\"A3\":10}}"
      val set=JsonFlatMapper.flatMap(json)
      assert(set.size() == 3)
    }
    "give correct keys" in{
      val json="{\"A1\":10,\"A2\":{\"A3\":10,\"A4\":{\"A5\":[1,2,3]}}}"
      val set=JsonFlatMapper.flatMap(json)
      assert(set.size() == 5)
    }
  }
}
