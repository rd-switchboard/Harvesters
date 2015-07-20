package org.rdswitchboard.libraries.records;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store relationship information
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-11
 * @version 1.0.0
 *
 * 
 */

public class Relationship {
	private static final String PROPERTY_FROM = "from";
	private static final String PROPERTY_TO = "to";
	private static final String PROPERTY_TYPE = "type";
	private final Map<String, Object> properties = new HashMap<String, Object>();
	
	/**
	 * Default class constructor
	 */
	public Relationship() {
	}
	
	/**
	 * Relationship constructor
	 * @param keyFrom From Key
	 * @param keyTo To key
	 * @param type Type name
	 */
	public Relationship(String from, String to, String type) {
		setFrom(from);
		setTo(to);
		setType(type);
	}
	
	/**
	 * Alternative Relationship constructor
	 * @param keyFrom From Key
	 * @param keyTo To key
	 * @param type Type name
	 * @param properties Additional properties (if null, please use other constructor)
	 */
	
	public Relationship(String from, String to, String type, Map<String, Object> properties) {
		setFrom(from);
		setTo(to);
		setType(type);
		setProperties(properties);
	}

	/**
	 * Return From Key
	 * @return String
	 */
	public String getFrom() {
		return (String) properties.get(PROPERTY_FROM);
	}

	/**
	 * Set From key
	 * @param keyFrom
	 */
	public void setFrom(String from) {
		properties.put(PROPERTY_FROM, from);
	}

	/**
	 * Set Ket From
	 * @param keyFrom
	 * @return Relationship
	 */
	public Relationship withFrom(String from) {
		setFrom(from);
		return this;
	}

	/**
	 * Returns Key To
	 * @return Key To
	 */
	public String getTo() {
		return (String) properties.get(PROPERTY_TO);
	}

	/**
	 * Sets Key To
	 * @param keyTo
	 */
	public void setTo(String to) {
		properties.put(PROPERTY_TO, to);
	}

	/**
	 * Sets Key To
	 * @param keyTo
	 * @return Relationship
	 */
	public Relationship withTo(String to) {
		setTo(to);
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
	public Relationship withType(String type) {
		setType(type);
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
	 */
	public Relationship withProperties(Map<String, Object> properties) {
		setProperties(properties);
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
	 * @return Relationship
	 */
	public Relationship withProperty(String key, Object value) {
		setProperty(key, value);
		return this;
	}
	
	/**
	 * 
	 */
	public void clear() {
		properties.clear();
	}

	@Override
	public String toString() {
		return "Relationship " + properties;
	}
}
