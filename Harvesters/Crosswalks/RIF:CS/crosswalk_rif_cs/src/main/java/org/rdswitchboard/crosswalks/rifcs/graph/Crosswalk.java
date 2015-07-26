package org.rdswitchboard.crosswalks.rifcs.graph;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.StatusType;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphRelationship;
import org.rdswitchboard.libraries.graph.GraphSchema;
import org.rdswitchboard.libraries.graph.GraphUtils;

import au.org.ands.standards.rif_cs.registryobjects.Activity;
import au.org.ands.standards.rif_cs.registryobjects.Collection;
import au.org.ands.standards.rif_cs.registryobjects.IdentifierType;
import au.org.ands.standards.rif_cs.registryobjects.NameType;
import au.org.ands.standards.rif_cs.registryobjects.Party;
import au.org.ands.standards.rif_cs.registryobjects.RegistryObjects;
import au.org.ands.standards.rif_cs.registryobjects.RelatedObjectType;
import au.org.ands.standards.rif_cs.registryobjects.RelationType;
import au.org.ands.standards.rif_cs.registryobjects.Service;

public class Crosswalk {	
	private static final String COLLECTION_TYPE_DATASET = "dataset";
	private static final String COLLECTION_TYPE_NON_GEOGRAOPHIC_DATASET = "nonGeographicDataset";
	private static final String COLLECTION_TYPE_RESEARCH_DATASET = "researchDataSet";
	
	private static final String ACTIVITY_TYPE_PROJECT = "project";
	private static final String ACTIVITY_TYPE_PROGRAM = "program";
	private static final String ACTIVITY_TYPE_AWARD = "award";
	
	private static final String PARTY_TYPE_PERSON = "person";
	private static final String PARTY_TYPE_PUBLISHER = "publisher";
	private static final String PARTY_TYPE_GROUP = "group";
	private static final String PARTY_TYPE_ADMINISTRATIVE_POSITION = "administrativePosition";
	
	private static final String IDENTIFICATOR_NLA = "AU-ANL:PEAU";
	private static final String IDENTIFICATOR_LOCAL = "local";
	
	private static final String NAME_PRIMARY = "primary";
	
	private static final String NAME_PART_FAMILY = "family";
	private static final String NAME_PART_GIVEN = "given";
	private static final String NAME_PART_SUFFIX = "suffix";
	private static final String NAME_PART_TITLE = "title";

	private Unmarshaller unmarshaller;
	private int existingRecords;
	private int deletedRecords;
	private int brokenRecords;
	
	private boolean verbose = false;
	
	public Crosswalk() throws JAXBException {
		unmarshaller = JAXBContext.newInstance( "org.openarchives.oai._2:au.org.ands.standards.rif_cs.registryobjects:au.org.ands.standards.rif_cs.extendedregistryobjects" ).createUnmarshaller();
	}
	
	public int getExistingRecords() {
		return existingRecords;
	}

	public int getDeletedRecords() {
		return deletedRecords;
	}

	public int getBrokenRecords() {
		return brokenRecords;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public Graph process(String source, InputStream xml) throws Exception {
		existingRecords = deletedRecords = brokenRecords = 0;
		
		JAXBElement<?> element = (JAXBElement<?>) unmarshaller.unmarshal( xml );
		Graph graph = new Graph();
		graph.addSchema(new GraphSchema(source, GraphUtils.PROPERTY_KEY, true));
		
		OAIPMHtype root = (OAIPMHtype) element.getValue();
		ListRecordsType records = root.getListRecords();
		if (null != records &&  null != records.getRecord()) {
			for (RecordType record : records.getRecord()) {
				HeaderType header = record.getHeader();
					
				StatusType status = header.getStatus();
				boolean deleted = status == StatusType.DELETED;
			
				if (null != record.getMetadata()) {
					Object metadata = record.getMetadata().getAny();
					if (metadata instanceof RegistryObjects) {
						RegistryObjects registryObjects = (RegistryObjects) metadata;
						if (registryObjects.getRegistryObject() != null && registryObjects.getRegistryObject().size() > 0) {
							for (RegistryObjects.RegistryObject registryObject : registryObjects.getRegistryObject()) {
								//String group = registryObject.getGroup();
								String key = registryObject.getKey();
								if (verbose) 
									System.out.println("Key: " + key);
								GraphNode node = new GraphNode()
									.withKey(key)
									.withSource(source);
								
								if (deleted) {
									graph.addNode(node.withProperty(GraphUtils.PROPERTY_DELETED, true));
									
									++deletedRecords;
								} else if (registryObject.getCollection() != null)
									importCollection(graph, node, registryObject.getCollection());
								else if (registryObject.getActivity() != null) 
									importActivity(graph, node, registryObject.getActivity());
								else if (registryObject.getService() != null)
									importService(graph, node, registryObject.getService());
								else if (registryObject.getParty() != null)
									importParty(graph, node, registryObject.getParty());	
								else {
									graph.addNode(node.withProperty(GraphUtils.PROPERTY_BROKEN, true));
									
									++brokenRecords;
								}
								
								++existingRecords;	
							}
								
							// at this point all registry objects should be imported, abort the function
						} else
							throw new Exception("Metadata does not contains any records");
					} else
						throw new Exception("Metadata is not in rif format");
				} else
					throw new Exception("Unable to find metadata");
			}
		} else
			System.out.println("Unable to find records");
		
		return graph;
	}
	
	private boolean importCollection(Graph graph, GraphNode node, Collection collection) {
		String type = collection.getType();
		if (type.equals(COLLECTION_TYPE_DATASET) 
				|| type.equals(COLLECTION_TYPE_NON_GEOGRAOPHIC_DATASET) 
				|| type.equals(COLLECTION_TYPE_RESEARCH_DATASET))
			node.setType(GraphUtils.TYPE_DATASET);
		else
			return false;// ignore
				
		for (Object object : collection.getIdentifierOrNameOrDates()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(graph, node, (RelatedObjectType) object);
		}
		
		graph.addNode(node);
		
		return true;
	}
	
	private boolean importActivity(Graph graph, GraphNode node, Activity activity) {
		String type = activity.getType();
		if (type.equals(ACTIVITY_TYPE_PROJECT) 
				|| type.equals(ACTIVITY_TYPE_PROGRAM) 
				|| type.equals(ACTIVITY_TYPE_AWARD))
			node.setType(GraphUtils.TYPE_GRANT);
		else
			return false;// ignore
				
		for (Object object : activity.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(graph, node, (RelatedObjectType) object);
		}
		
		graph.addNode(node);
		
		return true;
	}
	
	private boolean importService(Graph graph, GraphNode node, Service service) {
		return false; // ignore all
	}
	
	private boolean importParty(Graph graph, GraphNode node, Party party) {
		String type = party.getType();
		if (type.equals(PARTY_TYPE_PERSON) || type.equals(PARTY_TYPE_PUBLISHER))
			node.setType(GraphUtils.TYPE_RESEARCHER);
		else if (type.equals(PARTY_TYPE_GROUP) || type.equals(PARTY_TYPE_ADMINISTRATIVE_POSITION))
			node.setType(GraphUtils.TYPE_INSTITUTION);
		else
			return false;// ignore
				
		for (Object object : party.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(graph, node, (RelatedObjectType) object);
		}
		
		graph.addNode(node);
		
		return true;
	}
	
	private void processIdentifier(GraphNode node, IdentifierType identifier) {
		String type = identifier.getType();
		if (null != type) {
			if (type.equals(IDENTIFICATOR_NLA))
				type = GraphUtils.PROPERTY_NLA;
			else if (type.equals(IDENTIFICATOR_LOCAL))
				type = GraphUtils.PROPERTY_LOCAL_ID;
			else if (!type.equals(GraphUtils.PROPERTY_DOI) 
					&& !type.equals(GraphUtils.PROPERTY_ORCID) 
					&& !type.equals(GraphUtils.PROPERTY_PURL))
				type = null;			
		}
		
		if (null != type) {
			String key = identifier.getValue();
			if (null != key && !key.isEmpty())
				node.addProperty(type, key);
		}
	}
	
	private void processName(GraphNode node, NameType name) {
		String type = name.getType();
		if (null != type && type.equals(NAME_PRIMARY)) {
			String fullName = getFullName(name);
			if (!fullName.isEmpty())
				node.addProperty(GraphUtils.PROPERTY_TITLE, fullName);
		}
	}

	private String getFullName(NameType name) {
		String family = null;
		String given = null;
		String title = null;
		String suffix =  null;
		
		for (NameType.NamePart part : name.getNamePart()) {
			final String type = part.getType();
			if (null != type && !type.isEmpty()) {
				if (type.equals(NAME_PART_FAMILY))
					family = part.getValue();
				else if (type.equals(NAME_PART_GIVEN))
					given = part.getValue();
				else if (type.equals(NAME_PART_TITLE))
					title = part.getValue();
				else if (type.equals(NAME_PART_SUFFIX))
					suffix = part.getValue();
			}				
		}
		
		StringBuilder sb = new StringBuilder();
		if (null != title && !title.isEmpty()) 
			sb.append(title);
		if (null != suffix && !suffix.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(suffix);
		}
		if (null != given && !given.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(given);
		}
		if (null != family && !family.isEmpty()) {
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(family);
		}
		for (NameType.NamePart part : name.getNamePart()) {
			final String type = part.getType();
			if (null != type 
					&& !type.isEmpty() 
					&& (type.equals(NAME_PART_FAMILY) 
							|| type.equals(NAME_PART_GIVEN) 
							|| type.equals(NAME_PART_TITLE) 
							|| type.equals(NAME_PART_SUFFIX)))
				continue;
			
			if (sb.length() > 0)
				sb.append(" ");
			
			sb.append(part.getValue());				
		}
		
		return sb.toString();
	}
	
	private void processRelatedObject(Graph graph, GraphNode from, RelatedObjectType relatedObject) {
		for (RelationType relType : relatedObject.getRelation()) {
			String key = relatedObject.getKey();
			String type = relType.getType();
			if (null != key && !key.isEmpty() && null != type && !type.isEmpty()) { 
				Object source = from.getSource();
				GraphRelationship relationship = new GraphRelationship()
					.withRelationship(type)
					.withStartSource(source)
					.withStartKey(from.getKey())
					.withEndSource(source)
					.withEndKey(key);
				
				graph.addRelationship(relationship);
			}
		}
	}
}
