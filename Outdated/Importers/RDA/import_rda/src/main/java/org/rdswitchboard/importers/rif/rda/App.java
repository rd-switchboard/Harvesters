package org.rdswitchboard.importers.rif.rda;

import javax.xml.bind.JAXBException;

import org.rdswitchboard.importers.rif.Importer;

/**
 * Main class for RDA Rif data importer
 * This class require the import_rif library to be installed locally
 * 
 * This software design to process xml records in rda/xml/rif
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.3
 */

public class App {
	private static final String XML_FOLDER_URI = "rda/xml/rif";
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";
	
	/**
	 * Class main function 
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

			importer.importSets();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
}
