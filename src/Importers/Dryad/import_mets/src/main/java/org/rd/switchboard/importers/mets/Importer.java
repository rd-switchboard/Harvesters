package org.rd.switchboard.importers.mets;

import gov.loc.mets.MdSecType;
import gov.loc.mets.Mets;
import gov.loc.mets.MdSecType.MdWrap;
import gov.loc.mets.MdSecType.MdWrap.XmlData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

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
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.StatusType;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Mets importing library. Used by Dryad importer software
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Importer {
	protected static final String MD_TYPE_MODS = "MODS";
	
	protected static final String LABEL_RECORD = "Record";
	protected static final String LABEL_DRYAD = "Dryad";
	protected static final String LABEL_DRYAD_RECORD = LABEL_DRYAD + "_" + LABEL_RECORD;
	
	protected static final String PROPERTY_OAI = "oai";
	protected static final String PROPERTY_TIMESTAMP = "timestamp";
	protected static final String PROPERTY_DOI = "doi";
	protected static final String PROPERTY_NODE_SOURCE = "node_source";
	protected static final String PROPERTY_NODE_TYPE = "node_type";
	protected static final String PROPERTY_IDENTIFIER = "identifier";
	protected static final String PROPERTY_TITLE = "title";
	protected static final String PROPERTY_DESCRIPTION = "description";
	protected static final String PROPERTY_GENRE = "genre";
	protected static final String PROPERTY_NAME = "name";
	protected static final String PROPERTY_KEYWORDS = "keywords";
	protected static final String PROPERTY_N = "n";
	
	protected static final String NODE_IDENTIFIER = "identifier";
	protected static final String NODE_TITLE_INFO = "titleInfo";
	protected static final String NODE_ABSTRACT = "abstract";
	protected static final String NODE_NOTE = "note";
	protected static final String NODE_GENRE = "genre";
	protected static final String NODE_NAME = "name";
	protected static final String NODE_ROLE = "role";	
	protected static final String NODE_ROLE_TERM = "roleTerm";
	protected static final String NODE_NAME_PART = "namePart";
	protected static final String NODE_SUBJECT = "subject";
	protected static final String NODE_TOPIC = "topic";
	protected static final String NODE_RELATED_ITEM = "relatedItem";
	
	protected static final String ATTRIBUTE_TYPE = "type";
	
	protected static final String PART_DOI = "doi:";
	
	protected static final String GENRE_UNKNOWN = "unknown";
	
	protected static final String RELATION_RELATED_TO = "relatedTo";
	
	protected static final String CYPHER_FIND_NODE_BY_DOI = "MATCH (n:Dryad:Record) WHERE has(n.doi) and any (m in n.doi WHERE m = {doi}) RETURN n";
	
	private final String folderUri;
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> indexDryadRecord;
//	private RestIndex<Node> indexRecord;
	
	private Label labelRecord = DynamicLabel.label(LABEL_RECORD);
	private Label labelRDA = DynamicLabel.label(LABEL_DRYAD);

	private Unmarshaller unmarshaller;
	
	private int deletedRecords = 0;
	private int brokenRecords = 0;
	private int createdRecords = 0;
	private int createdRelationships = 0;
	
	private List<DoiRelation> relations = new ArrayList<DoiRelation>();
	
	/**
	 * Class constructor
	 * 
	 * @param folderUri path to xml folder
	 * @param neo4jUrl Neo4J url
	 * @throws JAXBException
	 */
	public Importer( final String folderUri, final String neo4jUrl ) throws JAXBException {
		
		graphDb = new RestAPIFacade(neo4jUrl); //"http://localhost:7474/db/data/");  
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_DRYAD_RECORD + ") ASSERT n." + PROPERTY_OAI + " IS UNIQUE", Collections.<String, Object> emptyMap());
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_DRYAD_RECORD + ") ASSERT n." + PROPERTY_DOI + " IS UNIQUE", Collections.<String, Object> emptyMap());

		indexDryadRecord = graphDb.index().forNodes(LABEL_DRYAD_RECORD);
//		indexRecord = graphDb.index().forNodes(LABEL_RECORD);
		
		unmarshaller = JAXBContext.newInstance( "org.openarchives.oai._2:gov.loc.mets" ).createUnmarshaller();
		
		this.folderUri = folderUri;		
	}		
	
	/**
	 * Function to perform records import
	 */
	public void importRecords() {
		deletedRecords = 0;
		brokenRecords = 0;
		createdRecords = 0;
		createdRelationships = 0;
		
		File[] folders = new File(folderUri).listFiles();
		for (File folder : folders) 
			if (folder.isDirectory() && !folder.getName().equals("_cache")) {
				File[] files = folder.listFiles();
				for (File file : files)  
					if (!file.isDirectory()) 				
						importRecord(file);
			}
		
		
		for (DoiRelation relation : relations) {
			System.out.println("Relationship [" + relation.type + "]: " + relation.doi);
			
			List<RestNode> hits = findNodeByDoi(relation.doi);
			if (null != hits) {
				RestNode nodeFrom = graphDb.getNodeById(relation.nodeId);
			
				for (RestNode node : hits) 
					createUniqueRelationship(nodeFrom, node, DynamicRelationshipType.withName(relation.type), null);
			}
		}
		
		System.out.println("Done. Detected " + createdRecords + " valid, " + deletedRecords + " deleted and " + brokenRecords + " broken records and " + createdRelationships + " relationships.");
	}
	
	private List<RestNode> findNodeByDoi(final String doi) {
		List<RestNode> result = null;
		
		Map<String, Object> pars = new HashMap<String, Object>();
		pars.put(PROPERTY_DOI, doi);
		
		QueryResult<Map<String, Object>> nodes = engine.query(CYPHER_FIND_NODE_BY_DOI, pars);
		for (Map<String, Object> row : nodes) {
			RestNode node = (RestNode) row.get(PROPERTY_N);
			if (null != node) {
				if (null == result)
					result = new ArrayList<RestNode>();
				
				result.add(node);
			}
		}
		
		return result;
	}
	
	/*
	private static Element findXmlElement(Element xml, String name) {
		if (xml.getLocalName().equals(name))
			return xml;

		return null;
	}
	*/
	
	private static Element findXmlElement(List<Object> xmls, String name) {
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
				return (Element) xml;

		return null;
	}
	
	private static Element findXmlElement(NodeList xmls, String name) {
		if (null != xmls) {
			int length = xmls.getLength();
			for (int i = 0; i < length; ++i) {
				org.w3c.dom.Node xml = xmls.item(i);
				if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
					return (Element) xml;
			}
		}

		return null;
	}	
	
	private static List<Element> findXmlElements(List<Object> xmls, String name) {
		List<Element> list = null;		
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}

		return list;
	}
	
	private static List<Element> findXmlElements(NodeList xmls, String name) {
		List<Element> list = null;		
		
		int length = xmls.getLength();
		for (int i = 0; i < length; ++i) {
			org.w3c.dom.Node xml = xmls.item(i);
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}
		}

		return list;
	}
	
	@SuppressWarnings("unchecked")
	private static void addData(Map<String, Object> map, String field, String data) {
		if (null != field && null != data && !data.isEmpty()) {
			Object par = map.get(field);
			if (null == par) 
				map.put(field, data);
			else {
				if (par instanceof String) {
					if (((String)par).equals(data))
						return; // we already have this string
					
					Set<String> pars = new HashSet<String>();
					pars.add((String) par);
					pars.add(data);
					map.put(field, pars);
				} else 
					((Set<String>)par).add(data);
			}
		}
	}
		
	@SuppressWarnings("unchecked")
	private void importRecord(File fileXml) {
		try {
			JAXBElement<?> element = (JAXBElement<?>) unmarshaller.unmarshal( new FileInputStream( fileXml ) );
			
			RecordType record = (RecordType) element.getValue();	
			HeaderType header = record.getHeader();
			
			StatusType status = header.getStatus();
			if (status == StatusType.DELETED)
			{
				++deletedRecords;
				return;
			}
			
			String idetifier = header.getIdentifier();
			System.out.println("Record: " + idetifier.toString());
			String datestamp = header.getDatestamp();
//			System.out.println(datestamp.toString());
//			List<String> specs = header.getSetSpec();
			
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(PROPERTY_OAI, idetifier);
			map.put(PROPERTY_TIMESTAMP, datestamp);
			map.put(PROPERTY_NODE_SOURCE, LABEL_DRYAD);
			map.put(PROPERTY_NODE_TYPE, LABEL_RECORD);
			
			if (null != record.getMetadata()) {
				Object metadata = record.getMetadata().getAny();
				if (metadata instanceof Mets) {
					Mets mets = (Mets) metadata;
					for (MdSecType dmdSec : mets.getDmdSec()) {
//						System.out.println(dmdSec.getID().toString());
						MdWrap mdWrap = dmdSec.getMdWrap();
//						System.out.println(mdWrap.getMDTYPE().toString());
						
						if (mdWrap.getMDTYPE().equals(MD_TYPE_MODS)) {
														
							XmlData xmlData = mdWrap.getXmlData();
							
							List<Object> xmlObjects = xmlData.getAny();
						/*	for (Object xmlObject : xmlObjects) {
								System.out.println(((Element) xmlObject).getLocalName().toString());
								System.out.println(xmlObject.getClass().toString());
							}*/
							
							List<Element> identifiers = findXmlElements(xmlObjects, NODE_IDENTIFIER);
							if (null != identifiers) {
								for (Element identifier : identifiers) {
									String type = identifier.getAttribute(ATTRIBUTE_TYPE);
									String identifierString = identifier.getTextContent();
									if (null != identifierString && ! identifierString.isEmpty()) {
										if (identifierString.contains(PART_DOI))
											addData(map, PROPERTY_DOI, identifierString);
										else if (null != type && !type.isEmpty()) 
											addData(map, PROPERTY_IDENTIFIER + "_" + type, identifierString);
										else
											addData(map, PROPERTY_IDENTIFIER, identifierString);
									}														
								}
							}
							
							Element title = findXmlElement(xmlObjects, NODE_TITLE_INFO);
							if (null != title)
								addData(map, PROPERTY_TITLE, title.getTextContent());
							
							Element description = findXmlElement(xmlObjects, NODE_ABSTRACT);
							if (null != description)
								addData(map, PROPERTY_DESCRIPTION, description.getTextContent());

							Element note = findXmlElement(xmlObjects, NODE_NOTE);
							if (null != note)
								addData(map, PROPERTY_DESCRIPTION, note.getTextContent());
								
							Element genre = findXmlElement(xmlObjects, PROPERTY_GENRE);
							if (null != genre) 
								addData(map, PROPERTY_GENRE, genre.getTextContent());
							else
								addData(map, PROPERTY_GENRE, GENRE_UNKNOWN);
							
							List<Element> names = findXmlElements(xmlObjects, NODE_NAME);
							if (null != names)
								for (Element name : names) {
									String roleString = null;
									String nameString = null;
									
									Element role = findXmlElement(name.getChildNodes(), NODE_ROLE);
									if (null != role) {
										Element roleTerm = findXmlElement(role.getChildNodes(), NODE_ROLE_TERM);
										if (null != roleTerm)
											roleString = roleTerm.getTextContent();																
									}
									List<Element> nameParts = findXmlElements(name.getChildNodes(), NODE_NAME_PART);
									if (null != nameParts) 
										for (Element namePart : nameParts) {
											if (null != nameString)
												nameString += " " + namePart.getTextContent();
											else
												nameString = namePart.getTextContent();
										}
									
									if (null != nameString && !nameString.isEmpty()) {
										if (null == roleString || roleString.isEmpty())
											roleString = PROPERTY_NAME;
									
										addData(map, roleString, nameString);
									}
								}
							
							List<Element> subjects = findXmlElements(xmlObjects, NODE_SUBJECT);
							if (null != subjects) 
								for (Element subject : subjects) {
									Element topic = findXmlElement(subject.getChildNodes(), NODE_TOPIC);
									if (null != topic)
										addData(map, PROPERTY_KEYWORDS, topic.getTextContent());
								}
							
							List<Element> relatedItems = findXmlElements(xmlObjects, NODE_RELATED_ITEM);
							if (null != relatedItems)
								for (Element relatedItem : relatedItems) {
									String relation = relatedItem.getTextContent();
									if (null != relation && !relation.isEmpty()) {
										String relationType = relatedItem.getAttribute(ATTRIBUTE_TYPE);
									
										if (null == relationType || relationType.isEmpty())
											relationType = RELATION_RELATED_TO;
									
										addData(map, relationType, relation);
									}
								}
																
							RestNode node = graphDb.getOrCreateNode(indexDryadRecord, PROPERTY_OAI, idetifier, map);
							Object dois = map.get(PROPERTY_DOI);
							if (null != dois) 
								if (dois instanceof String) {
									indexDryadRecord.add(node, PROPERTY_DOI, dois);
								} else {
									for (String doi : (Set<String>) dois) 
										indexDryadRecord.add(node, PROPERTY_DOI, doi);
								}
							if (!node.hasLabel(labelRecord))
								node.addLabel(labelRecord); 
							if (!node.hasLabel(labelRDA))
								node.addLabel(labelRDA);
							
							if (null != relatedItems)
								for (Element relatedItem : relatedItems) {
									String doi = relatedItem.getTextContent();
									if (null != doi && doi.contains(PART_DOI)) {
										DoiRelation rel = new DoiRelation();
										rel.nodeId = node.getId();
										rel.doi = doi;
										rel.type = relatedItem.getAttribute(ATTRIBUTE_TYPE);
										if (null == rel.type || rel.type.isEmpty())
											rel.type = RELATION_RELATED_TO;
										
										relations.add(rel);
									}
								}
							
							++createdRecords;
							
							return;
						} else
							System.out.println("The XML data is not in MODS format");
					}
				} else
					System.out.println("Metadata is not in mets format");
			} else
				System.out.println("Unable to find metadata");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
		++brokenRecords;
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
		
		++createdRelationships;
	}	
}
