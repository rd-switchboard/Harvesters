package org.rdswitchboard.importers.rda;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store information about RDA relation
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Relation {
	
	public static final String FIELD_REGISTRY_OBJECT_ID = "registry_object_id";
	public static final String FIELD_CLASS = "class";
	public static final String FIELD_RELATIONSHIP_TYPE = "relation_type";
	public static final String FIELD_RELATIONSHIP_DESCRIPTION = "relation_description";
	public static final String FIELD_RELATIONSHIP_URL = "relation_url";
	
	public static final String PROPERTY_RDA_CLASS = "rda_class";
	public static final String PROPERTY_DESCRIPTION = "description";
	public static final String PROPERTY_URL = "url";
	public static final String PROPERTY_RELATIONSHIP_SOURCE = "source";
	
	public static final String LABEL_RDA = "RDA";
	
	public String relatedObjectId;
	public String relationsipType;
	public Map<String, Object> data;
	
	public void addData(final String key, final String value) {
		if (null != value && !value.isEmpty()) {
			if (null == data)
				data = new HashMap<String, Object>();			
			data.put(key, value);
		}
	}
	
	public static Relation fromJson(Map<String, Object> json, final String relationshipType) throws Exception {
		String relatedObjectId = (String) json.get(FIELD_REGISTRY_OBJECT_ID);
		if (null != relatedObjectId && !relatedObjectId.isEmpty()) {
			Relation relationship = new Relation();
			
			relationship.relatedObjectId = relatedObjectId;
			relationship.relationsipType = (String) json.get(FIELD_RELATIONSHIP_TYPE);
			if (null == relationship.relationsipType || relationship.relationsipType.isEmpty())
				relationship.relationsipType = relationshipType;
			
			relationship.addData(PROPERTY_RDA_CLASS, (String) json.get(FIELD_CLASS));
			relationship.addData(PROPERTY_DESCRIPTION, (String) json.get(FIELD_RELATIONSHIP_DESCRIPTION));
			relationship.addData(PROPERTY_URL, (String) json.get(FIELD_RELATIONSHIP_URL));
			
			return relationship;
		} else
			throw new Exception("Unable to process relationship. Unknown related object id.");
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Relationship: [ Relatiopnship Type: " + relationsipType + ", Related Object Id: " + relatedObjectId);
		
		for (Map.Entry<String, Object> entry : data.entrySet()) {
			sb.append(", " + entry.getKey() + ": " + entry.getValue());
		}
		
		sb.append(" ]");
		
		return sb.toString();
	}
}
