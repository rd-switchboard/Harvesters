package org.rdswitchboard.utils.rds.records.clean;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import au.com.bytecode.opencsv.CSVReader;

public class App {
	
	public static void main(String[] args) {
		try {
			// read properties file
			Properties properties = new Properties();

			InputStream in = new FileInputStream("properties/mysql.properties");
			properties.load(in);
			in.close();

			InputStream in2 = new FileInputStream("properties/ands.properties");
			properties.load(in2);
			in2.close();

			String mHost = properties.getProperty("host", "localhost");
			String mUser = properties.getProperty("user");
			String mPassword = properties.getProperty("password");

			URL url = new URL(properties.getProperty("base_url") + "/registry_object/delete/");

			Cookie cookie = new Cookie("PHPSESSID", properties.getProperty("session"));

			int maxObjects = Integer.parseInt(properties.getProperty("max_objects", "256"));
			
			System.out.println("Loading index");
			Set<String> index = new HashSet<String>();
			
			// load data
			try (CSVReader reader = new CSVReader(new FileReader("rda_keys.csv"))) {
				
				String[] line;
	            boolean header = false;
	                        
	            while ((line = reader.readNext()) != null) 
	            {
	            	if (!header)
	                {
	            		header = true;
	                    continue;
	                }
	            	
	            	index.add(line[1] + ":" + line[0]);
	            }
			} 
			
			System.out.println("Loading JDBC driver");
			// The newInstance() call is a work around for some
	        // broken Java implementations
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			int deletedRows = 0;
			
			long beginTime = System.currentTimeMillis();
			
			Client client = Client.create();

			System.out.println("Connecting to the database");
			// Obtain the mysql connection
			try (Connection conn = DriverManager.getConnection("jdbc:mysql://"+mHost+"/dbs_registry?user="+mUser+"&password="+mPassword)) {
				System.out.println("Creating a statment");
				// create mysql statment
	            try (Statement stmt1 = conn.createStatement()) { //java.sql.ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
	            	System.out.println("Querying all data sources");
					// load all datasets objects
	            	try (ResultSet ds = stmt1.executeQuery("SELECT data_source_id FROM data_sources")) {
						while (ds.next()) {
							Integer sourceId = ds.getInt("data_source_id");
							System.out.println("Processing data source with id " + sourceId);
							
							List<Integer> arrIds = new ArrayList<Integer>();
							
							try (Statement stmt2 = conn.createStatement()) {
								try (ResultSet rs = stmt2.executeQuery("SELECT registry_object_id, slug FROM registry_objects WHERE data_source_id=" + sourceId + " AND class='collection'")) {
									 while (rs.next()) {
										 Integer objectId = rs.getInt("registry_object_id");
										 String slug = rs.getString("slug");
										 							 
										 if (!index.contains(objectId + ":" + slug)) {
											 arrIds.add(objectId);
										 }
									 }
								}
							}
							
							if (!arrIds.isEmpty()) {
								System.out.println("Found " + arrIds.size() + " objects");
								int steps = arrIds.size() / maxObjects + 1;
								for (int step = 0; step < steps; ++step) {
									
									StringBuilder sb = new StringBuilder();
									sb.append("data_source_id="+sourceId + "&select_all=false");
									addParam(sb, "filters[sort][updated]", "desc");
									addParam(sb, "filters[filter][status]", "PUBLISHED");
									
									int from = step * maxObjects;
									int to = Math.min(arrIds.size(), (step + 1) * maxObjects);
									for (int i = from; i < to; ++i) 
										addParam(sb, "affected_ids[]", arrIds.get(i).toString());
									
									System.out.println("Erasing " + (to - from) + " objects");
	
									String d = sb.toString();
								//	System.out.println(d);
											
									WebResource webResource = client.resource(url.toString());
									ClientResponse response = webResource
											 	.header("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0")
											 	.accept( MediaType.APPLICATION_JSON, "*/*" )
											 	.acceptLanguage( "en-US", "en" )
											 	.type( MediaType.APPLICATION_FORM_URLENCODED )
				                                .cookie(cookie)
									 			.post(ClientResponse.class, d);
									 
									if (response.getStatus() != 200) {
										throw new RuntimeException("Failed : HTTP error code : "
												+ response.getStatus());
									}

									String output = response.getEntity(String.class);
								 
									System.out.println(output);
								 
									deletedRows += to - from;
								}
							}
						}
					}
	            } 
			}
            	
			long endTime = System.currentTimeMillis();
			
			System.out.println(String.format("Done. Deleted %d records. Spent %d ms", 
					deletedRows, endTime - beginTime));
			
		} catch (Exception e) {
			e.printStackTrace();
	    }
	}
	
	public static void addParam(StringBuilder sb, String param, String value) throws UnsupportedEncodingException {
		sb.append("&");
		sb.append(URLEncoder.encode(param, "UTF-8"));
		sb.append("=");
		sb.append(value);
	}
}
