package org.rdswitchboard.libraries.graph;

import java.util.ArrayList;
import java.util.List;

public class Graph {
	private List<GraphNode> nodes;
	private List<GraphRelationship> relationships;
	private List<GraphSchema> schemas;
	
	public List<GraphNode> getNodes() {
		return nodes;
	}

	public void setNodes(List<GraphNode> nodes) {
		this.nodes = nodes;
	}

	public List<GraphRelationship> getRelationships() {
		return relationships;
	}

	public void setRelationships(List<GraphRelationship> relationships) {
		this.relationships = relationships;
	}

	public List<GraphSchema> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<GraphSchema> schemas) {
		this.schemas = schemas;
	}

	public void addNode(GraphNode node) {
		if (null == nodes) 
			nodes = new ArrayList<GraphNode>();
		nodes.add(node);
	}
	
	public void addRelationship(GraphRelationship relationship) {
		if (null == relationships) 
			relationships = new ArrayList<GraphRelationship>();
		relationships.add(relationship);
	}
	
	public void addSchema(GraphSchema schema) {
		if (null == schemas) 
			schemas = new ArrayList<GraphSchema>();
		schemas.add(schema);
	}

	@Override
	public String toString() {
		return "Graph [nodes=" + nodes + ", relationships=" + relationships
				+ ", schemas=" + schemas + "]";
	}	
}
