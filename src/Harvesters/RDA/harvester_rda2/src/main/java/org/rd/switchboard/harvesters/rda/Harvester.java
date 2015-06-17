package org.rd.switchboard.harvesters.rda;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

/**
 * RDA JSON Harvester
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.0
 */
public class Harvester {
	private static final int MAX_ROWS = 200;
	private static final String BASE_URL = "https://researchdata.ands.org.au/registry/services/api/registry_objects/";
	private static final String LIST_OBJECTS_URL = BASE_URL + "?rows=%d&fl=id&start=%d";
	private static final String GET_OBJECT_URL = BASE_URL + "%s/core-relationships";
	
	private static final String FILED_STATUS = "status";
	private static final String FILED_MESSAGE = "message";
	private static final String FILED_ERROR = "error";
	private static final String FIELD_MSG = "msg";
	private static final String FIELD_TRACE = "trace";
	private static final String FILED_RESPONSE = "response";
	
	private static final String STATUS_SUCCESS ="success";
			
	private static final ObjectMapper mapper = new ObjectMapper();   
	private static final TypeReference<LinkedHashMap<String, Object>> linkedHashMapTypeReference = new TypeReference<LinkedHashMap<String, Object>>() {};   

	private String folderJson;
	
	/**
	 * Class Constructor
	 * @param folderJson A folder to store JSON files
	 */
	public Harvester( final String folderJson ) {
		this.folderJson = folderJson;
				
		new File(folderJson).mkdirs();
	}	
	
	/**
	 * Main function to harvest data
	 * @throws Exception
	 */
	public void harvest() throws Exception {
		getObjects();
	}
	
	@SuppressWarnings("unchecked")
	private void getObjects() throws Exception {
		int numFound = 0; 
		int from = 0;
				
		do 
		{
			Map<String, Object> response = (Map<String, Object>) getNestedObject(listObjects(from, MAX_ROWS), FILED_RESPONSE);
			if (null != response) {
				RecordSet recordSet = RecordSet.fromJson(response);
				if (null != recordSet && recordSet.getProcessed() > 0) {
					if (0 == numFound)
						numFound = recordSet.getFound();
							
					for (String recordId : recordSet.getRecordIds()) 						
						saveObject(recordId);
						
					from += recordSet.getProcessed();
				} else 
					break;
			}
		} while (from < numFound);
	}
	

	private Map<String, Object> listObjects( final int from, final int size) {
		for (int i = 0; i < 10; ++i) {
			try {
				Map<String, Object> json = getJson(String.format(LIST_OBJECTS_URL, size, from));
				if (null != json)
					return json;
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	private void saveObject( final String id ) {
		
		for (int i = 0; i < 10; ++i) {
			try {
				String json = getString(String.format(GET_OBJECT_URL, id));
				String fileName = folderJson + "/" + id + ".json";
				
				BufferedWriter br = new BufferedWriter(new FileWriter(fileName));
				
				br.write(json);
				br.close();
				
				return;
			} catch (Exception e) {
				e.printStackTrace();
			}		
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
		}
	}
	
	@SuppressWarnings("unchecked")
	private Object getNestedObject(Map<String, Object> json, final String propertyName) throws Exception {
		if (null != json) {
			String status = (String) json.get(FILED_STATUS);
			if (null != status && status.equals(STATUS_SUCCESS)) {
				Map<String, Object> message = (Map<String, Object>) json.get(FILED_MESSAGE);
				if (null != message) {
					Map<String, Object> error = (Map<String, Object>) message.get(FILED_ERROR);
					if (null != error) 
						processError((String) error.get(FIELD_MSG), (String) error.get(FIELD_TRACE));
					else
						return message.get(propertyName);
				}
				else
					throw new Exception("Invalid response format, unable to find message data");
			} else
				throw new Exception("Invalid response status");
		} else
			throw new Exception("Invalid response");
		
		return null;
	}
	
	private void processError( final String msg, final String trace) throws Exception {
		throw new Exception ("Error: " + msg + ", Trace: " + trace);
	}
		
	private Map<String, Object> getJson( final String url ) {
		System.out.println("Downloading: " + url);
				
		ClientResponse response = Client.create()
								  .resource( url )
								  .accept( MediaType.APPLICATION_JSON ) 
								  .type( MediaType.APPLICATION_JSON )
								  .get( ClientResponse.class );
		
		try {
			return mapper.readValue( response.getEntity( String.class ), linkedHashMapTypeReference);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
    }   
	
	private String getString( final String url ) {
		System.out.println("Downloading: " + url);
				
		ClientResponse response = Client.create()
								  .resource( url )
								  .accept( MediaType.APPLICATION_JSON ) 
								  .type( MediaType.APPLICATION_JSON )
								  .get( ClientResponse.class );
		
		try {
			return response.getEntity( String.class );
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
    }   
}
