package org.rdswitchboard.harvesters.pmh.s3;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.amazonaws.util.StringUtils;

public class App {

	private static final String PROPERTIES_FILE = "properties/harvester.properties";
	
	public static void main(String[] args) {
		// init server uri
		String propertiesFile = PROPERTIES_FILE;
		if (args.length > 0 && !args[0].isEmpty())
			propertiesFile = args[0];
		
		try {
			Properties properties = new Properties();
			try (InputStream in = new FileInputStream(propertiesFile)) {
                properties.load(in);
			}
			
			Harvester harvester = new Harvester(properties);
			
			harvester.identify();

			if (StringUtils.isNullOrEmpty(harvester.getMetadataPrefix())) {
				// List and display supported metadata formats
				List<MetadataFormat> formats = harvester.listMetadataFormats();
				System.out.println("Supported metadata formats:");
				for (MetadataFormat format : formats) {
					System.out.println(format.toString());
				}
			} else 
				harvester.harvest();
			
			// Set default encoding for MARC21
			//harvester.setEncoding(ENCODING);
			
			/*
			try {
				// harvest OAI:DC
				harvester.harvest(MetadataPrefix.oai_dc);
				// harvest MARC21 
				harvester.harvest(MetadataPrefix.marcxml);
			} catch (Exception e) {
				e.printStackTrace();
			}*/
			
			harvester.printStatistics(System.out);
			
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
}
