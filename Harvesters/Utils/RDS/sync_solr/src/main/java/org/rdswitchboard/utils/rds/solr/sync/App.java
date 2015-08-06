package org.rdswitchboard.utils.rds.solr.sync;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class App {
	private static final String PROPERTIES_FILE = "properties/sync_solr.properties";
	private static final String DEFULT_DATA_SOURCE = "1";
	private static final String DEFULT_ATTEMPTS = "10";
	private static final String DEFULT_DELAY = "30";
		
	private static final ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		try {
			String propertiesFile = PROPERTIES_FILE;
	        if (args.length > 0 && !StringUtils.isEmpty(args[0])) 
	        	propertiesFile = args[0];
	
	        Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        System.out.println("Sync RD-Switchboard SOLR Index");
	                	        
	        String baseUrl = properties.getProperty("base.url");
	        if (StringUtils.isEmpty(baseUrl))
	            throw new IllegalArgumentException("Base URL can not be empty");
	        System.out.println("Base URL: " + baseUrl);
	        
			int dataSource = Integer.parseInt(properties.getProperty("data_source_id", DEFULT_DATA_SOURCE));
	        System.out.println("Data Source ID: " +  dataSource);

			int attempts = Integer.parseInt(properties.getProperty("attempts", DEFULT_ATTEMPTS));
			int delay = Integer.parseInt(properties.getProperty("delay", DEFULT_DELAY));


	        String sessionId = properties.getProperty("session");
	        if (StringUtils.isEmpty(sessionId))
	            throw new IllegalArgumentException("Session ID can not be empty");
	        
			Cookie cookie = new Cookie("PHPSESSID", sessionId);
			
			Client client = Client.create();
			
			Analyze a = analyze(client, baseUrl, dataSource, cookie);
			
			
			for (int chunk = 0; chunk < a.getNumChunk(); ++chunk) {
				int attempt = 0;
				while(true) {
					try {
						sync(client, baseUrl, dataSource, chunk, cookie);
						
						break;
					} catch (Exception e) {
						e.printStackTrace();
						
						if (++attempt > attempts)
							throw new Exception("Maximum number of attempts has been reached");
						
						Thread.sleep(delay);
					}
				}
			}

	        
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	private static Analyze analyze(Client client, String baseUrl, int dataSource, Cookie cookie) throws JsonParseException, JsonMappingException, IOException {
		System.out.println("Analyzing data source: " + dataSource);
		
		String url = baseUrl + "/registry/maintenance/smartAnalyze/sync/" + dataSource;
		String ref = baseUrl + "/registry/maintenance/syncmenu";
		
		System.out.println(url);
				
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource
			 	.header("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0")
			 	.header("Cache-Control", "no-cache, must-revalidate")
			 	.header("Referer", ref)
			 	.accept( MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, MediaType.WILDCARD )
			 	.acceptLanguage( "en-US", "en" )
			 	.type( MediaType.TEXT_PLAIN )
                .cookie(cookie)
	 			.post(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
		
		String output = response.getEntity(String.class);
		Analyze a = mapper.readValue(output, Analyze.class);
		
		System.out.println("Total Records: " + a.getTotal());
		System.out.println("Chunk Size: " + a.getChunkSize());
		System.out.println("Total Chunks: " + a.getNumChunk());
		
		return a;
	}
	
	private static void sync(Client client, String baseUrl, int dataSource, int chunk, Cookie cookie) {
		System.out.println("Syncing chunk: " + chunk);
		
		String url = baseUrl + "/registry//maintenance/smartSyncDS/sync/" +  dataSource + "/" + chunk;
		String ref = baseUrl + "/registry/maintenance/syncmenu";
		
		System.out.println(url);
		
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource
			 	.header("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0")
			 	.header("Cache-Control", "no-cache, must-revalidate")
			 	.header("Referer", ref)
			 	.accept( MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, MediaType.WILDCARD )
			 	.acceptLanguage( "en-US", "en" )
                .cookie(cookie)
	 			.get(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
}
