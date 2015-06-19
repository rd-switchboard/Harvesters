package org.rdswitchboard.harvesters.pmh.rda;

import java.util.List;

import org.rdswitchboard.harvesters.pmh.Harvester;
import org.rdswitchboard.harvesters.pmh.MetadataFormat;
import org.rdswitchboard.harvesters.pmh.MetadataPrefix;

/**
 * RDA OAI:PMH Harvesting Utility
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class App {
	private static final String REPO_URI = "http://researchdata.ands.org.au/registry/services/oai";
	private static final String FOLDER_XML = "rda/xml";

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
			// Create Harvester object
			Harvester harvester = new Harvester(repoUri, folderXml);
			
			// List supporred metadata formats
			List<MetadataFormat> formats = harvester.listMetadataFormats();
			System.out.println("Supported metadata formats:");
			for (MetadataFormat format : formats) {
				System.out.println(format.toString());
			}
			
			// harvest data
			harvester.harvestSimple(MetadataPrefix.rif);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}