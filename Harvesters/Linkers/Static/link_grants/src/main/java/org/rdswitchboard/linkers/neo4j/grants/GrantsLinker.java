package org.rdswitchboard.linkers.neo4j.grants;

import java.io.PrintStream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.libraries.neo4j.Neo4jUtils;

public class GrantsLinker {
	private GraphDatabaseService graphDb;
	
	private RelationshipType relKnownAs = DynamicRelationshipType.withName( GraphUtils.RELATIONSHIP_KNOWN_AS );
	
	private boolean verbose = false;
	private long processed = 0;
	private long linked = 0;
	private long skyped = 0;
	
	public GrantsLinker(final String neo4jFolder) {
		graphDb = Neo4jUtils.getGraphDb( neo4jFolder );
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void link() throws Exception {
		linkNodes(GraphUtils.SOURCE_ARC, GraphUtils.SOURCE_ANDS, GraphUtils.TYPE_GRANT);
		linkNodes(GraphUtils.SOURCE_NHMRC, GraphUtils.SOURCE_ANDS, GraphUtils.TYPE_GRANT);
	}
	
	private void linkNodes(String src, String dst, String type) {
		if (verbose)
			System.out.println("Linkng " + src + " with "+ dst);
		
		Label labelSource = DynamicLabel.label( src );
		Label labelType = DynamicLabel.label( type );
		
		try ( Transaction tx = graphDb.beginTx() ) {
			Index<Node> index = graphDb.index().forNodes(dst);
			
			ResourceIterator<Node> nodes = graphDb.findNodes( labelSource );
			while (nodes.hasNext()) {
				Node node = nodes.next();
				if (node.hasLabel(labelType)) {
					String key = (String) node.getProperty( GraphUtils.PROPERTY_KEY );
					long id = node.getId();
					boolean linkFound = false;
					if (verbose)
						System.out.println("Processing node: " + id + " with key: " + key);
				
					IndexHits<Node> hits = index.get(GraphUtils.PROPERTY_KEY, key);
					for (Node hit : hits) {
						if (id != hit.getId()) {
							if (verbose)
								System.out.println("Establish a link with node: " + hit.getId());

							Neo4jUtils.createUniqueRelationship(node, hit, relKnownAs, Direction.BOTH, null);
							
							++linked;
							linkFound = true;
						}
					}
					
					if (!linkFound)
						++skyped;					
					++processed;
				}
			}
			
			tx.success();
		}
	}
	
	public void printStatistics(PrintStream out) {
		out.println("Processed " + processed + ", linked " + linked + " and skyped " + skyped + " nodes");
	}
	
}
