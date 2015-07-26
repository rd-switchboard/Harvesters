package org.rdswitchboard.libraries.graph;

import java.util.Map;

/**
 * A class to store single Relationship between two nodes
 * 
 * It consists from:
 *  - relationship: a type of relationship
 *  - start: a set of properties, describing start node
 *  - end: a set of properties, describing end node 
 *  - this: an optional set of relationship properties.
 *  
 * Typically, nodes properties consists from two fields:
 *  - source: name of the source and used to distinguish different subsets
 *  - key: unique key of the node within source. If source does not exists,
 *         the key must be unique within a database.  
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-24
 * @version 1.0.0
 */

public class GraphRelationship extends GraphProperties {
    private String relationship;
    private final GraphProperties start = new GraphProperties();
    private final GraphProperties end = new GraphProperties();
    
    public GraphRelationship() {
    	
    }
     
/*    public GraphRelationship(String relationship, GraphProperties start, GraphProperties end) {
    	this.relationship = relationship;
    	this.start = start;
    	this.end = end;
    }
    
    public GraphRelationship(String relationship, GraphProperties start, GraphProperties end, Map<String, Object> properties) {
    	super(properties);
    	
    	this.relationship = relationship;
    	this.start = start;
    	this.end = end;
    }*/

	public GraphProperties getStart() {
		return start;
	}
	
	public Object getStartSource() {
		return start.getProperty(GraphUtils.PROPERTY_SOURCE);
	}

	public void setStartSource(Object source) {
		start.setProperty(GraphUtils.PROPERTY_SOURCE, source);
	}

	public Object getStartKey() {
		return start.getProperty(GraphUtils.PROPERTY_KEY);
	}

	public void setStartKey(Object key) {
		start.setProperty(GraphUtils.PROPERTY_KEY, key);
	}
	
	public GraphProperties getEnd() {
		return end;
	}

	public Object getEndSource() {
		return end.getProperty(GraphUtils.PROPERTY_SOURCE);
	}

	public void setEndSource(Object source) {
		end.setProperty(GraphUtils.PROPERTY_SOURCE, source);
	}

	public Object getEndKey() {
		return end.getProperty(GraphUtils.PROPERTY_KEY);
	}

	public void setEndKey(Object key) {
		end.setProperty(GraphUtils.PROPERTY_KEY, key);
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	public GraphRelationship withRelationship(String relationship) {
		setRelationship(relationship);
		return this;
	}
	
	public GraphRelationship withProperties(Map<String, Object> properties) {
		setProperties(properties);
		return this;
	}
	
	public GraphRelationship withProperty(String key, Object property) {
		setProperty(key, property);
		return this;
	}
	
	public GraphRelationship withStartSource(Object source) {
		setStartSource(source);
		return this;
	}

	public GraphRelationship withStartKey(Object key) {
		setStartKey(key);
		return this;
	}

	public GraphRelationship withEndSource(Object source) {
		setEndSource(source);
		return this;
	}

	public GraphRelationship withEndKey(Object key) {
		setEndKey(key);
		return this;
	}

	@Override
	public String toString() {
		return "GraphRelationship [relationship=" + relationship + ", start="
				+ start + ", end=" + end + ", properties=" + properties + "]";
	}
}
