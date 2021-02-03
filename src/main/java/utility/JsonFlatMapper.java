package utility;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class JsonFlatMapper {

    public static Map<String,Object> flatMap(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser parser = mapper.createParser(jsonString);
        Map<Object, Object> jsonObject = mapper.readValue(parser, Map.class);
        return getUniqueKeys(jsonObject);
    }

    private static Map<String,Object> getUniqueKeys(Object jsonObject) {
        if (jsonObject == null) return new HashMap<>();
        else {
            Map<String,Object> allKeys = new LinkedHashMap<>();
            if (jsonObject instanceof Map) {
                allKeys.putAll((Map) jsonObject);
                Set keys = ((Map<Object, Object>) jsonObject).keySet();
                for (Object key : keys) {
                    Map subMap = getUniqueKeys(((Map<Object, Object>) jsonObject).get(key));
                    allKeys.putAll(subMap);
                }
            }
            return allKeys;
        }
    }
}
