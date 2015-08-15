package org.rdswitchboard.libraries.records;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store Record information
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-11
 * @version 1.0.0
 */

public class Record {
	private static final String PROPERTY_KEY = "key";
	private static final String PROPERTY_TYPE = "type";
	private static final String PROPERTY_SOURCE = "source";
	
	private final Map<String, Object> properties = new HashMap<String, Object>();
	
	/**
	 * Default class constructor
	 */
	public Record() {
		
	}

	/**
	 * 
	 * @param key
	 * @param type
	 */
	public Record(String key, String type) {
		setKey(key);
		setType(type);
	}

	/**
	 * 
	 * @param key
	 * @param type
	 * @param source
	 */
	public Record(String key, String type, String source) {
		setKey(key);;
		setType(type);
		setSource(source);
	}
	
	/**
	 * 
	 * @param key
	 * @param type
	 * @param properties
	 */
	public Record(String key, String type, Map<String, Object> properties) {
		setKey(key);
		setType(type);
		setProperties(properties);
	}

	/**
	 * 
	 * @param key
	 * @param type
	 * @param properties
	 */
	public Record(String key, String type, String source, Map<String, Object> properties) {
		setKey(key);
		setType(type);
		setSource(source);
		setProperties(properties);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getKey() {
		return (String) properties.get(PROPERTY_KEY);
	}

	/**
	 * 
	 * @param key
	 */
	public void setKey(String key) {
		properties.put(PROPERTY_KEY, key);
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public Record withKey(String key) {
		setKey(key);
		return this;
	}

	/**
	 * 
	 * @return
	 */
	public String getType() {
		return (String) properties.get(PROPERTY_TYPE);
	}

	/**
	 * 
	 * @param type
	 */
	public void setType(String type) {
		properties.put(PROPERTY_TYPE, type);
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public Record withType(String type) {
		setType(type);
		return this;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getSource() {
		return (String) properties.get(PROPERTY_SOURCE);
	}

	/**
	 * 
	 * @param source
	 */
	public void setSource(String source) {
		properties.put(PROPERTY_SOURCE, source);
	}
	
	/**
	 * 
	 * @param source
	 * @return
	 */
	public Record withSource(String source) {
		setSource(source);
		return this;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public Object getProperty(String key) {
		return properties.get(key);
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, Object value) {
		properties.put(key, value);
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Record withProperty(String key, Object value) {
		setProperty(key, value);
		return this;
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, Object> getProperties() {
		return properties;
	}

	/**
	 * 
	 * @param properties
	 */
	public void setProperties(Map<String, Object> properties) {
		this.properties.putAll(properties);
	}
	
	/**
	 * 
	 * @param properties
	 * @return
	 */
	public Record withProperties(Map<String, Object> properties) {
		setProperties(properties);
		return this;
	}
	
	/**
	 * 
	 */
	public void clear() {
		properties.clear();
	}
	
	/**
	 * 
	 */
	@Override
	public String toString() {
		return "Record " + properties;
	}
}
