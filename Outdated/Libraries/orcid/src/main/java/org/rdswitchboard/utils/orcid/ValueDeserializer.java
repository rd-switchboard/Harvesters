package org.rdswitchboard.utils.orcid;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

public class ValueDeserializer  extends JsonDeserializer<String> {

	private static final String VALUE = "value";
	
	@Override
	public String deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
			throws IOException, JsonProcessingException {
		ObjectCodec oc = jsonParser.getCodec();
		JsonNode node = oc.readTree(jsonParser);
		
		final JsonNode nodeValue = node.get(VALUE);
		if (null == nodeValue) 
			return null;
		
		return nodeValue.getTextValue();
	}
}
