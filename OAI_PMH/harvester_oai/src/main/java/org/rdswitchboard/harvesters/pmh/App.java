package org.rdswitchboard.harvesters.pmh;

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
			} else {
				boolean result = harvester.harvest();
				
				harvester.printStatistics(result, System.out);
								
				if (!result)
					System.exit(1);
			}

			
			
		} catch (Exception e) {
			System.err.println("Error [" + e.getClass().getName() + "]:" + e.getMessage());
			
			e.printStackTrace();
					
			System.exit(2);
		}
	}
}
