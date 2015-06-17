package org.rd.switchboard.importers.patterns;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Institutions Importer class
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Importer {
	
	private static final String LABEL_INSTITUTION = "Institution";
	private static final String LABEL_PATTERN = "Pattern";
	private static final String LABEL_WEB = "Web";
	private static final String LABEL_WEB_INSTITUTION = LABEL_WEB + "_" + LABEL_INSTITUTION;
	private static final String LABEL_WEB_PATTERN = LABEL_WEB + "_" + LABEL_PATTERN;
	
	private static final String RELATIONSHIP_PATTERN = "pattern";
	
	private static final String PROPERTY_KEY = "key"; 
	private static final String PROPERTY_NODE_SOURCE = "node_source";
	private static final String PROPERTY_NODE_TYPE = "node_type";
	private static final String PROPERTY_COUNTRY = "country";
	private static final String PROPERTY_STATE = "state";
	private static final String PROPERTY_TITLE = "title";
	private static final String PROPERTY_URL = "url";
	private static final String PROPERTY_HOST = "host";
	private static final String PROPERTY_PATTERN = "pattern";
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> indexWebInstitution;
	private RestIndex<Node> indexWebPattern;
//	private RestIndex<Node> indexRecord;
	
	private Label labelInstitution = DynamicLabel.label(LABEL_INSTITUTION);
	private Label labelPattern = DynamicLabel.label(LABEL_PATTERN);
	private Label labelWeb = DynamicLabel.label(LABEL_WEB);
	
	private RelationshipType relPattern = DynamicRelationshipType.withName(RELATIONSHIP_PATTERN);
	
	/**
	 * Class constructor. 
	 * @param neo4jUrl An URL to the Neo4J
	 */
	public Importer(final String neo4jUrl) {
		graphDb = new RestAPIFacade(neo4jUrl);
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_WEB_PATTERN + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		indexWebInstitution = graphDb.index().forNodes(LABEL_WEB_INSTITUTION);
		indexWebPattern = graphDb.index().forNodes(LABEL_WEB_PATTERN);
	}

	/**
	 * Function to import instititions from an CSV file.
	 * For every line in the file, except a header line, an instace of Web:Institution will be 
	 * created. Institution URL will be used as an unique node key. The nodes with the same key 
	 * will NOT be overwritten.
	 * @param institutionsCsv A path to institutions.csv file
	 */
	public void importPatterns(final String patternsCsv) {
		try 
		{
			CSVReader reader = new CSVReader(new FileReader(patternsCsv));
		
			String[] pattern;
			boolean header = false;
			while ((pattern = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (pattern.length != 2)
					continue;
				
				String url = pattern[0];
				String pat = pattern[1];
				
				System.out.println( url + " - " + pat );
		
				if (null != url && !url.isEmpty()) {
				
					URL hostUrl = null;
					
					try {
						hostUrl = new URL(url);
					} catch(MalformedURLException ex) {
						hostUrl = new URL("http://" + url);
					}
					
					String host = hostUrl.getHost();
					if (host.startsWith("www."))
						host = host.substring(4);
					else if (host.startsWith("www3."))
						host = host.substring(5);
					else if (host.startsWith("web."))
						host = host.substring(4);
					
					String key = pat;
					
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(PROPERTY_KEY, key);
					map.put(PROPERTY_NODE_SOURCE, LABEL_WEB);
					map.put(PROPERTY_NODE_TYPE, LABEL_PATTERN);
					map.put(PROPERTY_PATTERN, pat);
					map.put(PROPERTY_HOST, host);
					
					RestNode nodePattern = graphDb.getOrCreateNode(indexWebPattern, PROPERTY_KEY, key, map);
					if (!nodePattern.hasLabel(labelPattern))
						nodePattern.addLabel(labelPattern); 
					if (!nodePattern.hasLabel(labelWeb))
						nodePattern.addLabel(labelWeb);
					
					Map<String, Object> pars = new HashMap<String, Object>();
					pars.put("host", host);
						
			
					
					QueryResult<Map<String, Object>> articles = engine.query("MATCH (n:" + LABEL_WEB + ":" + LABEL_INSTITUTION + ") WHERE n.host={host} RETURN n", pars);
					for (Map<String, Object> row : articles) {
						RestNode institutionNode = (RestNode) row.get("n");
						if (null != institutionNode) {
							createUniqueRelationship(graphDb, institutionNode, nodePattern,
									relPattern, Direction.OUTGOING, null);
						}
					}
				}
			}
			
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private static void createUniqueRelationship(RestAPI graphDb, RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Direction direction, Map<String, Object> data) {

		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type, direction);		
		for (Relationship rel : rels) {
			switch (direction) {
			case INCOMING:
				if (rel.getStartNode().getId() == nodeEnd.getId())
					return;
			case OUTGOING:
				if (rel.getEndNode().getId() == nodeEnd.getId())
					return;				
			case BOTH:
				if (rel.getStartNode().getId() == nodeEnd.getId() || 
				    rel.getEndNode().getId() == nodeEnd.getId())
					return;
			}
		}
		
		if (direction == Direction.INCOMING)
			graphDb.createRelationship(nodeEnd, nodeStart, type, data);
		else
			graphDb.createRelationship(nodeStart, nodeEnd, type, data);
	}	
	
}
