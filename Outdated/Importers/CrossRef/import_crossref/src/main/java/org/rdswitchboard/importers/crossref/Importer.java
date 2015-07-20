package org.rdswitchboard.importers.crossref;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rdswitchboard.utils.crossref.Author;
import org.rdswitchboard.utils.crossref.CrossRef;
import org.rdswitchboard.utils.crossref.Item;
import org.rdswitchboard.utils.neo4j.Neo4jUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.util.QueryResult;

public class Importer {
	private static final String PROPERTY_REFERENCED_BY = "referenced_by";
	
	private static final String PROPERTY_URL = "url";
	private static final String PROPERTY_PREFIX = "prefix";
	private static final String PROPERTY_ISSN = "issn";
	private static final String PROPERTY_TITLE = "title";
	private static final String PROPERTY_SUBTITLE = "subtitle";
//	private static final String PROPERTY_SUBJECT = "subject";
//	private static final String PROPERTY_CONTAINER_TITLE = "title_container";
	private static final String PROPERTY_AUTHOR = "author";
	private static final String PROPERTY_EDITOR = "editor";
	private static final String PROPERTY_ISSUED = "issued";
	private static final String PROPERTY_DEPOSITED = "deposited";
	private static final String PROPERTY_INDEXED = "indexed";
	
	private static final String PROPERTY_SUFFIX = "suffix";
	private static final String PROPERTY_FAMILY_NAME = "family_name";
	private static final String PROPERTY_GIVEN_NAME = "given_name";
	private static final String PROPERTY_FULL_NAME = "full_name";
	private static final String PROPERTY_ORCID = "orcid";
		
	private static final String PART_DOI = "doi:";
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private CrossRef crossref;
	
	private RestIndex<Node> indexCrossrefPublication;
	private RestIndex<Node> indexCrossrefResearcher;
	
	private int crossrefPublications;
	private int crossrefResearchers;
	
	private Map<String, RestNode> nodesCrossref = new HashMap<String, RestNode>();
	
	private static enum Labels implements Label {
		CrossRef, Publication, Researcher
	};
	
	private static enum Relationhips implements RelationshipType {
		author, editor, knownAs
	};
		
	public Importer(final String neo4jUrl, final String cacheFolder) {
		System.out.println("Neo4j URL: " + neo4jUrl);
		System.out.println("CrossRef Cache folder: " + cacheFolder);
		
		// connect to graph database
		graphDb = new RestAPIFacade(neo4jUrl);  
				
		// Create cypher engine
		engine = new RestCypherQueryEngine(graphDb);  

		Neo4jUtils.createConstraint(engine, Labels.CrossRef, Labels.Publication);
		Neo4jUtils.createConstraint(engine, Labels.CrossRef, Labels.Researcher);
	
		indexCrossrefPublication = Neo4jUtils.getIndex(graphDb, Labels.CrossRef, Labels.Publication);
		indexCrossrefResearcher = Neo4jUtils.getIndex(graphDb, Labels.CrossRef, Labels.Researcher);
		
		crossref = new CrossRef();
		crossref.setCacheFolder(cacheFolder);
	}
	
	public void process() {
		crossrefPublications = crossrefResearchers = 0;
		
		processNodes("Dryad:Publication", PROPERTY_REFERENCED_BY);
		
		System.out.println("Done. Imported " + crossrefPublications + " publications and " + crossrefResearchers + " researchers");
	}
	
	@SuppressWarnings("unchecked")
	private void processNodes(String label, String property) {
		StringBuilder sb = new StringBuilder();
		sb.append("MATCH (n:");
		sb.append(label);
		sb.append(") WHERE HAS(n.");
		sb.append(property);
		sb.append(") RETURN ID(n) AS id, n.");
		sb.append(property);
		sb.append(" AS doi");
		
		QueryResult<Map<String, Object>> result = engine.query(sb.toString(), null);
		for (Map<String, Object> row : result) {
			int nodeId = (int) row.get("id");
			Object dois = row.get("doi");
			if (null != dois) {
				RestNode node = graphDb.getNodeById(nodeId);
				if (null != node) {
					if (dois instanceof String) 
						processDoi(node, (String) dois);
					else if (dois instanceof String[])
						for (String doi :(String[]) dois) 
							processDoi(node, doi);
					else if (dois instanceof List<?>)
						for (String doi :(List<String>) dois) 
							processDoi(node, doi);
				}
			}
		}
	}

	private void processDoi(RestNode nodeStart, String doi) {
		if (doi.contains(PART_DOI)) {
			RestNode nodeCrossrefPublication = createCrossRefPublication(doi);
			if (null != nodeCrossrefPublication) {
				Neo4jUtils.createUniqueRelationship(graphDb, nodeStart, nodeCrossrefPublication, 
						Relationhips.knownAs, Direction.OUTGOING, null);	
			}
		}	
	}
	
	private RestNode createCrossRefPublication(final String doi) {
		System.out.println("Processing doi: " + doi);
		
		if (nodesCrossref.containsKey(doi))
			return nodesCrossref.get(doi);
		
		RestNode nodeCrossrefPublication = null;
		
		Item item = crossref.requestWork(doi);
		if (null != item) {
			Map<String, Object> map = new HashMap<String, Object>();
			addProperty(map, PROPERTY_URL, item.getUrl());
			addProperty(map, PROPERTY_PREFIX, item.getPrefix());
			addProperty(map, PROPERTY_ISSN, item.getIssn());
			addProperty(map, PROPERTY_TITLE, item.getTitle());
			addProperty(map, PROPERTY_SUBTITLE, item.getSubtitle());
//			addProperty(map, PROPERTY_SUBJECT, item.getSubject());
//			addProperty(map, PROPERTY_CONTAINER_TITLE, item.getContainerTitle());
			addProperty(map, PROPERTY_AUTHOR, item.getAuthorString());
			addProperty(map, PROPERTY_EDITOR, item.getEditorString());
			addProperty(map, PROPERTY_ISSUED, item.getIssuedString());
			addProperty(map, PROPERTY_DEPOSITED, item.getDepositedString());
			addProperty(map, PROPERTY_INDEXED, item.getIndexedString());
					
			nodeCrossrefPublication =  Neo4jUtils.createUniqueNode(graphDb, indexCrossrefPublication, 
						Labels.CrossRef, Labels.Publication, doi, map);
			
			System.out.println("Creating Crosref:Publication: " + doi);

			++crossrefPublications;
				
			if (null != item.getAuthor())
				for (Author author : item.getAuthor()) {
					RestNode nodeCrossrefResearcher = createCrossRefResearcher(doi, author);
					if (null != nodeCrossrefResearcher)
						Neo4jUtils.createUniqueRelationship(graphDb, nodeCrossrefResearcher, nodeCrossrefPublication, 
							Relationhips.author, Direction.OUTGOING, null);	
				}
			
			if (null != item.getEditor())
				for (Author editor : item.getEditor()) {
					RestNode nodeCrossrefResearcher = createCrossRefResearcher(doi, editor);
					
					if (null != nodeCrossrefResearcher)
						Neo4jUtils.createUniqueRelationship(graphDb, nodeCrossrefResearcher, nodeCrossrefPublication, 
								Relationhips.editor, Direction.OUTGOING, null);	
				}
		}
		else
			System.out.println("Unable to fina any crossref record by doi: " + doi);			
	
		nodesCrossref.put(doi, nodeCrossrefPublication);
		
		return nodeCrossrefPublication;
	}
	
	private RestNode createCrossRefResearcher(final String doi, Author author) {
		Map<String, Object> map = new HashMap<String, Object>();
		addProperty(map, PROPERTY_SUFFIX, author.getSuffix());
		addProperty(map, PROPERTY_GIVEN_NAME, author.getGiven());
		addProperty(map, PROPERTY_FAMILY_NAME, author.getFamily());
		addProperty(map, PROPERTY_FULL_NAME, author.getFullName());
		addProperty(map, PROPERTY_ORCID, author.getOrcid());
		
		++crossrefResearchers;
		
		String key = doi + ":" + author.getFullName();
		
		System.out.println("Creating Crosref:Researcher: " + key);
		
		return Neo4jUtils.createUniqueNode(graphDb, indexCrossrefResearcher, 
				Labels.CrossRef, Labels.Researcher, key, map);
	}
	
	private void addProperty(Map<String, Object> map, final String key, final Object value) {
		if (null != key && !key.isEmpty() && null != value) {
			if (value instanceof String && ((String) value).isEmpty())
				return;
			
			map.put(key, value);				
		}
	}
}
