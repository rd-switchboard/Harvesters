package org.rdswitchboard.harvesters.pmh.cern;

import java.util.List;

import javax.xml.bind.JAXBException;

import org.rdswitchboard.harvesters.pmh.Harvester;
import org.rdswitchboard.harvesters.pmh.MetadataFormat;
import org.rdswitchboard.harvesters.pmh.MetadataPrefix;

/**
 * CERN Harvesting Utility
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.4
 */

public class App {
	private static final String REPO_URI = "http://cds.cern.ch/oai2d.py/";
	private static final String FOLDER_XML = "cern/xml";
	private static final String ENCODING = "ISO-8859-1";

	/**
	 * Package main function
	 * @param args
	 */
	public static void main(String[] args) {
		
		// init server uri
		String repoUri = REPO_URI;
		if (args.length > 0 && !args[0].isEmpty())
			repoUri = args[0];
		
		// init folder address
		String folderXml = FOLDER_XML;
		if (args.length > 1 && !args[1].isEmpty())
			folderXml = args[1];
		
		try {
			// Create harvester object
			Harvester harvester = new Harvester(repoUri, folderXml);
			
			// List and display supported metadata formats
			List<MetadataFormat> formats = harvester.listMetadataFormats();
			System.out.println("Supported metadata formats:");
			for (MetadataFormat format : formats) {
				System.out.println(format.toString());
			}

			// Set default encoding for MARC21
			harvester.setEncoding(ENCODING);
			
			try {
				// harvest OAI:DC
				harvester.harvest(MetadataPrefix.oai_dc);
				// harvest MARC21 
				harvester.harvest(MetadataPrefix.marcxml);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (JAXBException e1) {
			e1.printStackTrace();
		}
	}
}
