package org.rdswitchboard.harvesters.pmh.dryad;

import java.util.List;

import org.rdswitchboard.harvesters.pmh.Harvester;
import org.rdswitchboard.harvesters.pmh.MetadataFormat;
import org.rdswitchboard.harvesters.pmh.MetadataPrefix;

/**
 * Dryad Harvesting Utility
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.1
 */
public class App {
	private static final String REPO_URI = "http://www.datadryad.org/oai/request";
	private static final String FOLDER_XML = "dryad/xml";
	
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

			// List supported formats
			List<MetadataFormat> formats = harvester.listMetadataFormats();
			System.out.println("Supported metadata formats:");
			for (MetadataFormat format : formats) {
				System.out.println(format.toString());
			}
			
			// harvest
			harvester.harvestSimple(MetadataPrefix.oai_dc);
			harvester.harvestSimple(MetadataPrefix.mets);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
