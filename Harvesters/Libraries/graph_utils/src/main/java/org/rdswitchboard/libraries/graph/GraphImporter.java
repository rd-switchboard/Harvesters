package org.rdswitchboard.libraries.graph;

import java.util.Collection;

public interface GraphImporter {
	void importGraph(Graph graph);
	
	void importNode(GraphNode node);
	void importNodes(Collection<GraphNode> nodes);
	
	void importSchema(GraphSchema schema);
	void importSchemas(Collection<GraphSchema> schemas); 
	
	void importRelationship(GraphRelationship relationship);
	void importRelationships(Collection<GraphRelationship> relationships);
	
	boolean isVerbose();
	void setVerbose(boolean verbose);
}
