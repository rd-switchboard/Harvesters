package org.rdswitchboard.importers.rda;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store information about RDA record
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Record {
	
	public static final String RDA_HOST = "http://researchdata.ands.org.au/";
	
	public static final String FIELD_ID = "id";
	public static final String FIELD_KEY = "key";
	public static final String FIELD_SLUG = "slug";
	public static final String FIELD_CLASS = "class";
	
	public static final String PROPERTY_RDA_ID = "rda_id";
	public static final String PROPERTY_RDA_KEY = "rda_key";
	public static final String PROPERTY_RDA_SLUG = "rda_slug";
	public static final String PROPERTY_RDA_CLASS = "rda_class";
	public static final String PROPERTY_RDA_URL = "rda_url";
	public static final String PROPERTY_NODE_TYPE = "node_type";
	public static final String PROPERTY_NODE_SOURCE = "node_source";
	
	public static final String LABEL_RECORD = "Record";
	public static final String LABEL_RDA = "RDA";
	public static final String LABEL_RDA_RECORD = LABEL_RDA + "_" + LABEL_RECORD;
	
	public Map<String, Object> data = new HashMap<String, Object>();
	
	public static String getPropertyName(final String fieldName) {
		if (fieldName.equals(FIELD_ID))
			return PROPERTY_RDA_ID;
		if (fieldName.equals(FIELD_KEY))
			return PROPERTY_RDA_KEY;
		if (fieldName.equals(FIELD_SLUG))
			return PROPERTY_RDA_SLUG;
		if (fieldName.equals(FIELD_CLASS))
			return PROPERTY_RDA_CLASS;
		return fieldName;
	}
	
	public static Record fromJson(Map<String, Object> json) {
		Record record = new Record();
		
		for (Map.Entry<String, Object> entry : json.entrySet()) {
			String value = (String) entry.getValue();
			if (null != value && !value.isEmpty())
				record.data.put(getPropertyName(entry.getKey()), value);
		}	
		
		record.data.put(PROPERTY_RDA_URL, record.getUrl());
		record.data.put(PROPERTY_NODE_TYPE, LABEL_RECORD);
		record.data.put(PROPERTY_NODE_SOURCE, LABEL_RDA);

		return record;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Record: [ ");
		
		boolean init = false;
		for (Map.Entry<String, Object> entry : data.entrySet()) {
			if (!init)
				init = true;
			else
				sb.append(", ");
			sb.append(entry.getKey() + ": " + entry.getValue());
		}
		
		sb.append(" ]");
		
		return sb.toString();
	}
	
	public String getId() {
		return (String) data.get(PROPERTY_RDA_ID);
	}
	
	public String getSlug() {
		return (String) data.get(PROPERTY_RDA_SLUG);
	}
	
	public String getUrl() {
		return RDA_HOST + getSlug() + "/" + getId();
	}
}
