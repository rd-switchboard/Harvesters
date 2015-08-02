package org.rdswitchboard.libraries.graph;

import java.util.Map;

/**
 * 
 * @author dima
 *
 */

public class GraphNode extends GraphProperties {

	public GraphNode() {
		
	}
	
	public GraphNode(Map<String, Object> properties) {
		super(properties);
	}
	
	public Object getKey() {
		return getProperty(GraphUtils.PROPERTY_KEY);
	}
	
	public void setKey(Object key) {
		setProperty(GraphUtils.PROPERTY_KEY, key);
	}
	
	public Object getSource() {
		return getProperty(GraphUtils.PROPERTY_SOURCE);
	}
	
	public void setSource(Object source) {
		setProperty(GraphUtils.PROPERTY_SOURCE, source);
	}

	public Object getType() {
		return getProperty(GraphUtils.PROPERTY_TYPE);
	}
	
	public void setType(Object type) {
		setProperty(GraphUtils.PROPERTY_TYPE, type);
	}
	
	public boolean isDeleted() {
		Object deleted = getProperty(GraphUtils.PROPERTY_DELETED);
		return null != deleted && (Boolean) deleted;
	}

	public boolean isBroken() {
		Object broken = getProperty(GraphUtils.PROPERTY_BROKEN);
		return null != broken && (Boolean) broken;
	}
	
	public void setDeleted(boolean deleted) {
		setProperty(GraphUtils.PROPERTY_DELETED, deleted);
	}

	public void setBroken(boolean broken) {
		setProperty(GraphUtils.PROPERTY_BROKEN, broken);
	}
	
	public GraphNode withProperties(Map<String, Object> properties) {
		setProperties(properties);
		return this;
	}
	
	public GraphNode withProperty(String key, Object property) {
		setProperty(key, property);
		return this;
	}
	
	public GraphNode withKey(String key) {
		setKey(key);
		return this;
	}
	
	public GraphNode withSource(String source) {
		setSource(source);
		return this;
	}

	public GraphNode withType(String type) {
		setType(type);
		return this;
	}
	
	public GraphNode withDeleted(boolean deleted) {
		setDeleted(deleted);
		return this;
	}

	public GraphNode withBroken(boolean broken) {
		setBroken(broken);
		return this;
	}

	@Override
	public String toString() {
		return "GraphNode [properties=" + properties + "]";
	}	
}
