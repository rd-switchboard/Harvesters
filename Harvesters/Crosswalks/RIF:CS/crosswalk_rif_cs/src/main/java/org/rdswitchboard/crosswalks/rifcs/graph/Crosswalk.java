package org.rdswitchboard.crosswalks.rifcs.graph;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.StatusType;
import org.rdswitchboard.libraries.record.Record;

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

	public Map<String, Record> process(InputStream xml) throws Exception {
		existingRecords = deletedRecords = brokenRecords = 0;
		
		JAXBElement<?> element = (JAXBElement<?>) unmarshaller.unmarshal( xml );
		Map<String, Record> graph = new HashMap<String, Record>();
		
		OAIPMHtype root = (OAIPMHtype) element.getValue();
		ListRecordsType records = root.getListRecords();
		if (null != records &&  null != records.getRecord()) {
			for (RecordType record : records.getRecord()) {
				HeaderType header = record.getHeader();
					
				StatusType status = header.getStatus();
				boolean deleted = status == StatusType.DELETED;
				
					
				//String idetifier = header.getIdentifier();
//				System.out.println("Record: " + idetifier.toString());
								
				if (null != record.getMetadata()) {
					Object metadata = record.getMetadata().getAny();
					if (metadata instanceof RegistryObjects) {
						RegistryObjects registryObjects = (RegistryObjects) metadata;
						if (registryObjects.getRegistryObject() != null && registryObjects.getRegistryObject().size() > 0) {
							for (RegistryObjects.RegistryObject registryObject : registryObjects.getRegistryObject()) {
								//String group = registryObject.getGroup();
								String key = registryObject.getKey();
								Record node = graph.get(key);
								if (null == node) {
									node = new Record().withKey(key);
									graph.put(key, node);
								} else
									throw new Exception("Duplicate key has been found");
								
								if (deleted) {
									node.setProperty("deleted", true);
									++deletedRecords;
								}
								
								boolean sound = false;
								
								if (registryObject.getCollection() != null)
									sound = importCollection(node, registryObject.getCollection());
								else if (registryObject.getActivity() != null) 
									sound = importActivity(node, registryObject.getActivity());
								else if (registryObject.getService() != null)
									sound = importService(node, registryObject.getService());
								else if (registryObject.getParty() != null)
									sound = importParty(node, registryObject.getParty());																				
								
								if (!sound) {
									node.setProperty("broken", true);
									++brokenRecords;
								}	
								
								++existingRecords;	
								if (verbose) {
									System.out.println("Key: " + key);
									System.out.println("Node: " + node);
								}								
							}
								
							// at this point all registry objects should be imported, abort the function
						} else
							throw new Exception("Metadata does not contains any records");
					} else
						throw new Exception("Metadata is not in rif format");
				} else
					throw new Exception("Unable to find metadata");
				
				++brokenRecords;
			}
		} else
			System.out.println("Unable to find records");
		
		return graph;
	}
	
	private boolean importCollection(Record node, Collection collection) {
		String type = collection.getType();
		if (type.equals("dataset") || type.equals("nonGeographicDataset") || type.equals("researchDataSet"))
			node.setType("dataset");
		else
			return false;// ignore
				
		for (Object object : collection.getIdentifierOrNameOrDates()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(node, (RelatedObjectType) object);
		}
		
		return true;
	}
	
	private boolean importActivity(Record node, Activity activity) {
		String type = activity.getType();
		if (type.equals("project") || type.equals("program") || type.equals("award"))
			node.setType("grant");
		else
			return false;// ignore
				
		for (Object object : activity.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(node, (RelatedObjectType) object);
		}
		
		return true;
	}
	
	private boolean importService(Record node, Service service) {
		return false; // ignore all
	}
	
	private boolean importParty(Record node, Party party) {
		String type = party.getType();
		if (type.equals("person") || type.equals("publisher"))
			node.setType("researcher");
		else if (type.equals("group") || type.equals("administrativePosition"))
			node.setType("institution");
		else
			return false;// ignore
				
		for (Object object : party.getIdentifierOrNameOrLocation()) {
			if (object instanceof IdentifierType) 
				processIdentifier(node, (IdentifierType) object);
			else if (object instanceof NameType)
				processName(node, (NameType) object);
			else if (object instanceof RelatedObjectType) 
				processRelatedObject(node, (RelatedObjectType) object);
		}
		
		return true;
	}
	
	private void processIdentifier(Record node, IdentifierType identifier) {
		String type = identifier.getType();
		if (null != type) {
			if (type.equals("AU-ANL:PEAU"))
				type = "nla";
			else if (type.equals("local"))
				type = "id";
			else if (!type.equals("doi") && !type.equals("orcid") && !type.equals("purl"))
				type = null;			
		}
		
		if (null != type) {
			String key = identifier.getValue();
			if (null != key && !key.isEmpty())
				node.addIndex(key, type);
		}
	}
	
	private void processName(Record node, NameType name) {
		String type = name.getType();
		if (null != type && type.equals("primary")) {
			String fullName = getFullName(name);
			if (!fullName.isEmpty())
				node.addProperty("title", fullName);
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
	
	private void processRelatedObject(Record node, RelatedObjectType relatedObject) {
		for (RelationType relType : relatedObject.getRelation()) {
			String key = relatedObject.getKey();
			String type = relType.getType();
			if (null != key && !key.isEmpty() && null != type && !type.isEmpty())
				node.addRelationship(relatedObject.getKey(), relType.getType());
		}
	}
}