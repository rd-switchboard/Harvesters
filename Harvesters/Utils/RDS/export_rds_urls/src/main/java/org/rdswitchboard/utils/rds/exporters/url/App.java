package org.rdswitchboard.utils.rds.exporters.url;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.mysql.jdbc.StringUtils;

// USER creation
// CREATE USER 'user_name'@'localhost' IDENTIFIED BY 'some_pass';
// GRANT SELECT ON dbs_registry.registry_objects TO 'user_name'@'localhost';

public class App {
	private static final String PROPERTIES_FILE = "properties/export_rds_url.properties";
	private static final String MYSQL_HOST = "localhost";
	private static final String MYSQL_DATABASE = "dbs_registry";
	private static final String OUTPUT_NAME = "rds_urls.csv";
	//private static final String URL_SCHEMA = "rd-switchboard.net/%s/%s";
	private static final String DELEMITER = ",";
	
	private static final String FIELD_KEY = "key";
	private static final String FIELD_DATA_SOURCE_ID = "data_source_id";
	private static final String FIELD_SLUG = "slug";
	private static final String FIELD_RECORD_ID = "registry_object_id";
		
	public static void main(String[] args) {
		try {
			String propertiesFile = PROPERTIES_FILE;
	        if (args.length > 0 && !StringUtils.isNullOrEmpty(args[0])) 
	        	propertiesFile = args[0];
	
	        Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        System.out.println("Exporting RD-Switchboard Registry Object URL's");
	                	        
	        String dbHost = properties.getProperty("mysql.host", MYSQL_HOST);
	        if (StringUtils.isNullOrEmpty(dbHost))
	            throw new IllegalArgumentException("MySQL Host can not be empty");

	        String dbUsername = properties.getProperty("mysql.username");
	        if (StringUtils.isNullOrEmpty(dbUsername))
	            throw new IllegalArgumentException("MySQL Username can not be empty");

	        String dbPassword = properties.getProperty("mysql.password");
	        if (StringUtils.isNullOrEmpty(dbPassword))
	            throw new IllegalArgumentException("MySQL Password can not be empty");

	        String dbDatabase = properties.getProperty("mysql.database", MYSQL_DATABASE);
	        if (StringUtils.isNullOrEmpty(dbDatabase))
	            throw new IllegalArgumentException("MySQL Database can not be empty");
	        
	        String outFileName = properties.getProperty("output.name", OUTPUT_NAME);
	        if (StringUtils.isNullOrEmpty(outFileName))
	            throw new IllegalArgumentException("Output file name can not be empty");
	        System.out.println("Output: " + outFileName);

	        String outDelemiter = properties.getProperty("output.delemiter", DELEMITER);
	        if (StringUtils.isNullOrEmpty(outDelemiter))
	            throw new IllegalArgumentException("Output delemiter can not be empty");

	        System.out.println("Loading JDBC driver");
            // The newInstance() call is a work around for some
	        // broken Java implementations
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            System.out.println("Connecting to the database");
            // Obtain the mysql connection
            try (Connection conn = DriverManager.getConnection("jdbc:mysql://"+dbHost+"/"+dbDatabase+"?user="+dbUsername+"&password="+dbPassword)) {
            	Map<Long, String> dataSources = queryDataSources(conn);
            	
            	try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            		try (ResultSet ds = stmt.executeQuery("SELECT `data_source_id`, `key`, slug, registry_object_id FROM registry_objects")) {
           	        	try (PrintWriter writer = new PrintWriter(outFileName, StandardCharsets.UTF_8.name())) {
           	        		writer.println("ds_id,ds,key,record_id,slug");
        		        	
           	        		while (ds.next()) {
           	        			Long dsId = ds.getLong(FIELD_DATA_SOURCE_ID);
           	        			
           	        			writer.print(dsId);
           	        			writer.print(outDelemiter);
           	        			writer.print(dataSources.get(dsId));
           	        			writer.print(outDelemiter);
           	        			writer.print(ds.getString(FIELD_KEY));
		                        writer.print(outDelemiter);
		                        writer.print(ds.getString(FIELD_SLUG));                          
		                        writer.print(outDelemiter);
		                        writer.println(ds.getLong(FIELD_RECORD_ID));
           	        		}
           	        	}                	   
            		}
	            }
	        }
            
		} catch (Exception e) {
			e.printStackTrace();
		}	

	}

	private static Map<Long, String> queryDataSources(Connection conn) throws SQLException {
		Map<Long, String> ds = new HashMap<Long, String>();
	
		try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (ResultSet rs = stmt.executeQuery("SELECT `data_source_id`, `key` FROM data_sources")) {
				while (rs.next()) {
					ds.put(rs.getLong(FIELD_DATA_SOURCE_ID), rs.getString(FIELD_KEY));
     		   }
			}
		}
		
		return ds;
	}
	
}
