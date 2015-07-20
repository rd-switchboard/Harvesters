package org.rdswitchboard.libraries.record;

import java.util.Map;

public class Record {
	private static final String PROPERTY_KEY = "key";
	private static final String PROPERTY_TYPE = "type";
	private static final String PROPERTY_SOURCE = "source";
	
	protected MapSet indexes = null;
	protected MapSet properties = null;
	protected MapSet relationships = null;
	
	public String getKey() {
		return (String) getProperty(PROPERTY_KEY);
	}

	public void setKey(String key) {
		addIndex(key, PROPERTY_KEY);
	}
	
	public String getType() {
		return (String) getProperty(PROPERTY_TYPE);
	}

	public void setType(String type) {
		setProperty(PROPERTY_TYPE, type);
	}
	
	public String getSource() {
		return (String) getProperty(PROPERTY_SOURCE);
	}
	
	public void setSource(String source) {
		setProperty(PROPERTY_SOURCE, source);
	}	
	
	public Map<String, Object> getProperties() {
		return properties;
	}
			
	public Map<String, Object> getIndexes() {
		return indexes;
	}
	
	public Map<String, Object> getRelationships() {
		return relationships;
	}
	
	public Object getProperty(String key) {
		if (null == properties)
			return null;
		return properties.get(key);
	}
	
	public void setProperty(String key, Object value) {
		if (null != value) {
			if (null == properties) 
				properties = new MapSet();
				
			properties.put(key, value);
		}
	}

	public void addProperty(String key, Object value) {
		if (null != value) {
			if (null == properties) 
				properties = new MapSet();
			properties.add(key, value);
		}
	}

		
	public Object getIndex(String index) {
		if (null == indexes)
			return null;
		return indexes.get(index);
	}
	
	public void addIndex(String index, String type) {
		if (null == indexes) 
			indexes = new MapSet();
		indexes.add(index, type);
		addProperty(type, index);
	}
	
	public Object getRelationship(String key) {
		if (null == relationships)
			return null;
		return relationships.get(key);
	}
	
	public void addRelationship(String key, String type) {
		if (null == relationships) 
			relationships = new MapSet();
		relationships.add(key, type);
	}
	
	public Record withKey(String key) {
		setKey(key);
		return this;
	}
	
	public Record withType(String type) {
		setType(type);
		return this;
	}
	
	public Record withSource(String source) {
		setSource(source);
		return this;
	}
	
	public Record withProperty(String key, Object value) {
		setProperty(key, value);
		return this;
	}
	
	public Record withRelationship(String key, String type) {
		addRelationship(key, type);
		return this;
	}

	public Record withIndex(String index, String type) {
		addIndex(index, type);
		return this;
	}
	
	public void clear() {
		indexes.clear();
		properties.clear();
		relationships.clear();
	}

	@Override
	public String toString() {
		return "Record [indexes=" + indexes + ", properties=" + properties
				+ ", relationships=" + relationships + "]";
	}
}