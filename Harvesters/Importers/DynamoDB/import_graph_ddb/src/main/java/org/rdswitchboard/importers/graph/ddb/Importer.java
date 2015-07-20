package org.rdswitchboard.importers.graph.ddb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rdswitchboard.libraries.record.Record;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class Importer {
	//public static final String TABLE_IMPORTS = "Import";
	public static final String TABLE_PROPERTIES = "PropertyValue";
	public static final String TABLE_INDEXES = "PropertyIndex";
	public static final String TABLE_RELATIONSHIPS = "Relationship";
	
	public static final String POROPERTY_KEY = "key";
	public static final String POROPERTY_KEY1 = "key1";
	public static final String POROPERTY_KEY2 = "key2";
	public static final String POROPERTY_SOURCE = "source";
	public static final String POROPERTY_INDEX = "index";
	public static final String POROPERTY_NAME = "name";
	public static final String POROPERTY_TYPE = "type";
	
    private DynamoDB dynamoDB;
    private Map<String, List<Item>> requests = new HashMap<String, List<Item>>();
    private int requestCounter = 0;
    private boolean verbose = false;
    
  //  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	public Importer(AWSCredentials awsCredentials) {
		AmazonDynamoDB client = new AmazonDynamoDBClient(awsCredentials);
		client.setRegion(Region.getRegion(Regions.US_WEST_2));
		dynamoDB = new DynamoDB(client);
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@SuppressWarnings("unchecked")
	public void importRecords(String source, Collection<Record> records) {
/*		Calendar cal = Calendar.getInstance();
		Table imports = dynamoDB.getTable("Import");
		
		imports.putItem(new Item()
			.withPrimaryKey("File", importName)
			.withString("Source", source)
			.withString("Status", "Processing")
			.withString("Date", dateFormat.format(cal.getTime()))
			);	*/	
		
	    for (Record record : records) {
	    	record.setProperty(POROPERTY_SOURCE, source);
	    	
	    	Map<String, Object> properties = record.getProperties();
	    	if (null != properties) {
	    		String recordKey = record.getKey();
	    		Item item =  new Item().withPrimaryKey(POROPERTY_KEY, recordKey);
	    		if (verbose) 
	    			System.out.println("Posting Record with key: " + recordKey + ", properties: " + properties);
	    		    	
		    	for (Map.Entry<String, Object> entry : properties.entrySet()) {
		    		String key = entry.getKey();
		    		Object value = entry.getValue();
		    		if (!key.equals(POROPERTY_KEY) && null != value) {
		    			if (value instanceof String)
	    					item.withString(key, (String) value);
	    				else if (value instanceof Boolean)
	    					item.withBoolean(key, (Boolean) value);
	    				else
	    					item.withStringSet(key, (Set<String>) value);
	    			}
		    	}
		    	
		    	if (verbose) 
		    	
		    	addItem(TABLE_PROPERTIES, item);
		    	
		    	Map<String, Object> indexes = record.getIndexes();
		    	if (null != indexes) {
			    	for (Map.Entry<String, Object> entry : indexes.entrySet()) {
			    		String key = entry.getKey();
			    		Object value = entry.getValue();
			    		
			    		if (verbose) 
			    			System.out.println("Posting Index with key: " + key + ", directed to: " + recordKey + ", with name: " + value);
			    	   					    		
			    		Item itemIndex = new Item()
			    			.withPrimaryKey(POROPERTY_INDEX, key)
			    			.withString(POROPERTY_KEY, recordKey);
			    		
			    		if (null != value) {
			    			if (value instanceof String)
			    				itemIndex.withString(POROPERTY_NAME, (String) value);
			   				else 
			   					itemIndex.withStringSet(POROPERTY_NAME, (Set<String>) value);
			    		}
			    		
		    			addItem(TABLE_INDEXES, itemIndex);
			    	}
		    	}
		    	
			    Map<String, Object> relationships = record.getRelationships();
			    if (null != relationships) {
			    	for (Map.Entry<String, Object> entry : record.getRelationships().entrySet()) {
			    		String key = entry.getKey();
			    		Object value = entry.getValue();

			    		if (verbose) 
			    			System.out.println("Posting Relationship  from: " + recordKey + ", to: " + key + ", with type: " + value);
			    	   					    		
			    		Item itemIndex = new Item()
			    			.withPrimaryKey(POROPERTY_KEY1, recordKey)
			    			.withString(POROPERTY_KEY2, key);
			    		
			    		if (null != value) {
			    			if (value instanceof String)
			    				itemIndex.withString(POROPERTY_TYPE, (String) value);
			   				else 
			   					itemIndex.withStringSet(POROPERTY_TYPE, (Set<String>) value);
			    		}
			    		
		    			addItem(TABLE_RELATIONSHIPS, itemIndex);
			    	}
			    }
	    	}
	    }

	    postItems();
	    
	/*	imports.putItem(new Item()
			.withPrimaryKey("File", importName)
			.withString("Source", source)
			.withString("Status", "Imported")
			.withString("Date", dateFormat.format(cal.getTime()))
		);	*/	    
	}
	
	protected void addItem(String table, Item item) {
		List<Item> items = requests.get(table);
		if (null == items) {
			items = new ArrayList<Item>();
			requests.put(table, items);
		}
		
		items.add(item);
		if (++requestCounter >= 25) 
			postItems();
	}
	
	protected void postItems() {
		if (requestCounter > 0) {
			List<TableWriteItems> witeItems = new ArrayList<TableWriteItems>();
			
			if (verbose) 
				System.out.println("Posting Batch Request:");
				
			for (Map.Entry<String, List<Item>> entry : requests.entrySet()) {
				if (verbose) 
					System.out.println("Table: " + entry.getKey() + " Items: " + entry.getValue());
				witeItems.add(new TableWriteItems(entry.getKey()).withItemsToPut(entry.getValue()));
			}
			
			BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(witeItems.toArray(new TableWriteItems[witeItems.size()]));
			while (outcome.getUnprocessedItems().size() > 0) {
				Map<String, List<WriteRequest>> uprocessedItems = outcome.getUnprocessedItems();
					
				if (verbose) {
					System.out.println("Batch request has been incomplete. Affected tables: ");
					for (Map.Entry<String,  List<WriteRequest>> entry : uprocessedItems.entrySet()) {
						System.out.println("* " + entry.getKey() + " - " + entry.getValue().size() + " reuqests peningd");
					}
				}
								
				outcome = dynamoDB.batchWriteItemUnprocessed(uprocessedItems);
			}  
			
			requests.clear();
			requestCounter = 0;
		}
	}
}