package org.rd.switchboard.importers.cern;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.StatusType;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Main class for CERN DC data importer
 *
 * The class uses automatically generated org.openarchives.oai._2 and org.purl.dc.elements._1
 *
 * This software design to process xml records in cern/xml/oai_dc
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class App {

	/**
	 * CERN data lables
	 */
	private static enum Labels implements Label {
		Publication, Cern
	};

	/**
	 * Function to test is XML element has correct local name
	 * @param xml Element
	 * @param name String
	 * @return Element or null if name is incorrect
	 */
	protected static Element findXmlElement(Element xml, String name) {
		if (xml.getLocalName().equals(name))
			return xml;

		return null;
	}
	
	/**
	 * Function to select correct xml element from a list
	 * @param xmls {@code List<Object> xmls}
	 * @param name String 
	 * @return Element or null if name is incorrect
	 */
	protected static Element findXmlElement(List<Object> xmls, String name) {
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name))
				return (Element) xml;

		return null;
	}
	
	/**
	 * Function to select correct xml element from a NodeList
	 * @param xmls NodeList
	 * @param name String
	 * @return Element or null if name is incorrect
	 */
	protected static Element findXmlElement(NodeList xmls, String name) {
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
	
	/**
	 * Function to select List of xml elements by local name
	 * @param xmls {@code List<Object>}
	 * @param name String
	 * @return {@code List<Element>}
	 */
	protected static List<Element> findXmlElements(List<Object> xmls, String name) {
		List<Element> list = null;		
		for (Object xml : xmls) 
			if (xml instanceof Element && ((Element) xml).getLocalName().equals(name)) {
				if (null == list)
					list = new ArrayList<Element>();
				
				list.add((Element) xml);
			}

		return list;
	}
	
	/**
	 * Function to add a property to the map. If there is already property with same name, 
	 * it will be converted to a Set 
	 * @param map {@code Map<String, Object>} A map to be changed
	 * @param field String
	 * @param data String
	 */
	@SuppressWarnings("unchecked")
	protected static void addData(Map<String, Object> map, String field, String data) {
		if (null != field && !field.isEmpty() && null != data && !data.isEmpty()) {
			Object par = map.get(field);
			if (null == par) 
				map.put(field, data);
			else if (par instanceof String) {
				List<String> pars = new ArrayList<String>();
				pars.add((String) par);
				pars.add(data);
				map.put(field, par);				
			} else 
				((List<String>)par).add(data);			
		}
	}
	
	/**
	 * Main function
	 * @param args String[] 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// connect to graph database
		RestAPI graphDb = new RestAPIFacade("http://localhost:7474/db/data/");  
		RestCypherQueryEngine engine=new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:Cern_Publication) ASSERT n.doi IS UNIQUE", Collections.<String, Object> emptyMap());
		RestIndex<Node> index = graphDb.index().forNodes("Cern_Publication");
		
		try {
			JAXBContext jc = JAXBContext.newInstance( "org.openarchives.oai._2:org.purl.dc.elements._1" );
			Unmarshaller u = jc.createUnmarshaller();
			
			File[] folders = new File("cern/xml/oai_dc").listFiles();
			for (File folder : folders)
				if (folder.isDirectory() && !folder.getName().equals("_cache")) {
					File[] files = folder.listFiles();
					for (File file : files)  
						if (!file.isDirectory()) {				
							try {
								JAXBElement<?> element = (JAXBElement<?>) u.unmarshal( new FileInputStream( file ) );
								
								RecordType record = (RecordType) element.getValue();	
								HeaderType header = record.getHeader();
								
								if (header.getStatus() != StatusType.DELETED) {
									
									String idetifier = header.getIdentifier();
									System.out.println("idetifier: " + idetifier);
	//								System.out.println(idetifier.toString());
								//	String datestamp = header.getDatestamp();
		//							System.out.println(datestamp.toString());
								//	List<String> specs = header.getSetSpec();
									
									if (null != idetifier && !idetifier.isEmpty() && null != record.getMetadata()) {
										Element metadata = (Element) record.getMetadata().getAny();
										if (null != metadata) {
											Map<String, Object> map = new HashMap<String, Object>();
											
											for(org.w3c.dom.Node child = metadata.getFirstChild(); child != null; child = child.getNextSibling())
										        if(child instanceof Element) {
										        	String tag = ((Element) child).getLocalName();
										        	if (tag.equals("identifier")) {
										        		if (child.getTextContent().contains("cds.cern.ch"))
										        			addData(map, "cern_url", child.getTextContent());
										        	} else if (tag.equals("language")) {
										        		addData(map, "language", child.getTextContent());
										        	} else if (tag.equals("title")) {
										        		addData(map, "title", child.getTextContent());
										        	} else if (tag.equals("subject")) {
										        		addData(map, "subject", child.getTextContent());
										        	} else if (tag.equals("publisher")) {
										        		addData(map, "publisher", child.getTextContent());
										        	} else if (tag.equals("date")) {
										        		addData(map, "date", child.getTextContent());
										        	}
										        }
										
											map.put("doi", idetifier);
											map.put("node_source", "Cern");
											map.put("node_type", "Publication");
											
										//	System.out.println("Create node");
											
											RestNode node = graphDb.getOrCreateNode(index, "doi", idetifier, map);
											if (!node.hasLabel(Labels.Publication))
												node.addLabel(Labels.Publication); 
											if (!node.hasLabel(Labels.Cern))
												node.addLabel(Labels.Cern);
										}											
									}
								}
							
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							}
						}
			}
			
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
