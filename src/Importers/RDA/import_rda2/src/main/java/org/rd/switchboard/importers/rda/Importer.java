package org.rd.switchboard.importers.rda;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

/**
 * RDA JSON Importer class
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Importer {
	
	private final String folderUri;
	
	private static final ObjectMapper mapper = new ObjectMapper();   
	private static final TypeReference<LinkedHashMap<String, Object>> linkedHashMapTypeReference = new TypeReference<LinkedHashMap<String, Object>>() {};   

	private static final String FIELD_STATUS = "status";
	private static final String FIELD_MESSAGE = "message";
	private static final String FIELD_ERROR = "error";
	private static final String FIELD_MSG = "msg";
	private static final String FIELD_TRACE = "trace";
	private static final String FIELD_REGISTRY_OBJECT = "registry_object";
	private static final String FIELD_RELATIONSHIPS = "relationships";
	
	private static final String STATUS_SUCCESS ="success";
	
	private static final String PART_COUNT = "_count";
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> index;
	
	private Label labelRecord = DynamicLabel.label(Record.LABEL_RECORD);
	private Label labelRDA = DynamicLabel.label(Record.LABEL_RDA);
	
	private int createdRecords = 0;
	private int createdRelations = 0;
	
	/**
	 * Class constructor
	 * @param folderUri String containing path to JSON folder 
	 * @param neo4jUrl String containing Neo4J URL
	 */
	public Importer( final String folderUri , final String neo4jUrl ) {
		this.folderUri = folderUri;
		
		new File(folderUri).mkdirs();
		
		graphDb = new RestAPIFacade(neo4jUrl); //"http://localhost:7474/db/data/");  
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + Record.LABEL_RDA_RECORD + ") ASSERT n." + Record.PROPERTY_RDA_ID + " IS UNIQUE", Collections.<String, Object> emptyMap());

		index = graphDb.index().forNodes(Record.LABEL_RDA_RECORD);
	}	
	
	/**
	 * Function to import JSON data
	 */
	public void importRecords() {
		createdRecords = 0;
		createdRelations = 0;
		
		File[] files = new File(folderUri).listFiles();
		for (File file : files) 
			if (!file.isDirectory()) 
				importRecord(file);
		
		for (File file : files) 
			if (!file.isDirectory()) 
				importRelationships(file);
		
		System.out.println("Done. Created " + createdRecords + " records and " + createdRelations + " relationships.");
	}
	
	/**
	 * Function to import single node from a JSON file
	 * @param file Link to a JSON file
	 */
	public void importRecord(File file) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> json = (Map<String, Object>) mapper.readValue(file, linkedHashMapTypeReference);
			if (null != json) {
				Map<String, Object> registryObject = getRegistryObject(json);
				if (null != registryObject) 
					createRecord(Record.fromJson(registryObject));
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Function to import relationships from a JSON file
	 * @param file Link to a JSON file
	 */
	@SuppressWarnings("unchecked")
	public void importRelationships(File file) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> json = (Map<String, Object>) mapper.readValue(file, linkedHashMapTypeReference);
			if (null != json) {
				Map<String, Object> registryObject = getRegistryObject(json);
				if (null != registryObject) {
					List<Map<String, Object>> relationships = getRelationships(json);
					if (null != relationships) {
						String recordId = (String) registryObject.get(Record.FIELD_ID);
						RestNode nodeFrom = findNodeById(recordId);

						if (null != nodeFrom) 
							for (Map<String, Object> relationship : relationships) 
								for (Map.Entry<String, Object> entry : relationship.entrySet()) 
									if (!entry.getKey().contains(PART_COUNT)) {
										List<Map<String, Object>> list = (List<Map<String, Object>>) entry.getValue();
										for (Map<String, Object> item : list) 
											createRelationship(nodeFrom, Relation.fromJson(item, entry.getKey()));								
									}
					}
				}
			}			
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getRegistryObject(Map<String, Object> json) throws Exception {
		if (null != json) {
			String status = (String) json.get(FIELD_STATUS);
			if (null != status && status.equals(STATUS_SUCCESS)) {
				Map<String, Object> message = (Map<String, Object>) json.get(FIELD_MESSAGE);
				if (null != message) {
					Map<String, Object> error = (Map<String, Object>) message.get(FIELD_ERROR);
					if (null != error) 
						processError((String) error.get(FIELD_MSG), (String) error.get(FIELD_TRACE));
					else
						return (Map<String, Object>) message.get(FIELD_REGISTRY_OBJECT);
				}
				else
					throw new Exception("Invalid response format, unable to find message data");
			} else
				throw new Exception("Invalid response status");
		} else
			throw new Exception("Invalid response");
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getRelationships(Map<String, Object> json) throws Exception {
		if (null != json) {
			Map<String, Object> message = (Map<String, Object>) json.get(FIELD_MESSAGE);
			if (null != message) 
				return (List<Map<String, Object>>) message.get(FIELD_RELATIONSHIPS);
			else
				throw new Exception("Invalid response format, unable to find message data");
		} else
			throw new Exception("Invalid response");
	}
	
	private void processError( final String msg, final String trace) throws Exception {
		throw new Exception ("Error: " + msg + ", Trace: " + trace);
	}
	

	private RestNode findNodeById(final String recordId) {
		IndexHits<Node> hist = index.get(Record.PROPERTY_RDA_ID, recordId);
		if (hist != null && hist.size() > 0)
			return (RestNode) hist.getSingle();
		return null;
	}
	
	private void createRecord(Record record) {
		System.out.println("Create Record: " + record.toString());
			
		RestNode node = graphDb.getOrCreateNode(index, Record.PROPERTY_RDA_ID, record.getId(), record.data);
		if (!node.hasLabel(labelRecord))
			node.addLabel(labelRecord); 
		if (!node.hasLabel(labelRDA))
			node.addLabel(labelRDA);
		
		++createdRecords;
	}
	
	private void createRelationship(RestNode nodeFrom, Relation relationship ) {
		System.out.println("Create Record: " + relationship.toString());
		
		RestNode nodeTo = findNodeById(relationship.relatedObjectId);
		if (null != nodeTo) 
			createUniqueRelationship(nodeFrom, nodeTo, 
					DynamicRelationshipType.withName(relationship.relationsipType), relationship.data);
		
		++createdRelations;
	}
	
	private void createUniqueRelationship(RestNode nodeStart, RestNode nodeEnd, 
			RelationshipType type, Map<String, Object> data) {

		// get all node relationships. They should be empty for a new node
		Iterable<Relationship> rels = nodeStart.getRelationships(type);		
		for (Relationship rel : rels) 
			if (rel.getStartNode().getId() == nodeStart.getId() && 
				rel.getEndNode().getId() == nodeEnd.getId())
				return;
		
		graphDb.createRelationship(nodeStart, nodeEnd, type, data);
	}	
}
