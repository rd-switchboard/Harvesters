package org.rdswitchboard.importers.mets.dryad;

import javax.xml.bind.JAXBException;

import org.rdswitchboard.importers.mets.Importer;

/**
 * Main class for Dryad Mets data importer
 * This class require the import_mets library to be installed locally
 * 
 * This software design to process xml records in dryad/xml/mets 
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.2
 */

public class App {
	
	private static final String XML_FOLDER_URI = "dryad/xml/mets";
	//private static final String XML_FOLDER_URI = "dryad-1";
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";
	
	/**
	 * Class Main function
	 * @param args String[]
	 */
	public static void main(String[] args) {

		String folderXml = XML_FOLDER_URI;
		if (args.length > 0 && !args[0].isEmpty())
			folderXml = args[0];
		
		String neo4jUrl = NEO4J_URL;
		if (args.length > 1 && !args[1].isEmpty())
			neo4jUrl = args[1];
			
		try {
			Importer importer = new Importer(folderXml, neo4jUrl);
			
			importer.importRecords();
			
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

}
