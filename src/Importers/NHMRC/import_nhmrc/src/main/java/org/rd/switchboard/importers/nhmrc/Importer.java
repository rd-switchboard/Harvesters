package org.rd.switchboard.importers.nhmrc;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.rd.switchboard.utils.graph.GraphRelationship;
import org.rd.switchboard.utils.graph.GraphUtils;

import au.com.bytecode.opencsv.CSVReader;

public class Importer {
	
	// CSV files are permanent, no problem with defining it
	private static final String GRANTS_CSV_PATH = "nhmrc/2014/grants-data.csv";
	private static final String ROLES_CSV_PATH = "nhmrc/2014/ci-roles.csv";
//	private static final int MAX_REQUEST_PER_TRANSACTION = 1000;

	private static final String LABEL_GRANT = "Grant";
	private static final String LABEL_RESEARCHER = "Researcher";
	private static final String LABEL_INSTITUTION = "Institution";

	private static final String LABEL_NHMRC = "NHMRC";
	private static final String LABEL_NHMRC_GRANT = LABEL_NHMRC + "_" + LABEL_GRANT;
	private static final String LABEL_NHMRC_RESEARCHER = LABEL_NHMRC + "_" + LABEL_RESEARCHER;
	private static final String LABEL_NHMRC_INSTITUTION = LABEL_NHMRC + "_" + LABEL_INSTITUTION;
	
	//private static final String LABEL_RDA = "NLA";
	//private static final String LABEL_RDA_INSTITUTION = LABEL_RDA + "_" + LABEL_INSTITUTION;

	private static final String FIELD_PURL = "purl";
//	private static final String FIELD_NLA = "nla";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_STATE = "state";
	private static final String FIELD_TYPE = "type";
//	private static final String FIELD_SOURCE = "source";

	private static final String FIELD_NHMRC_GRANT_ID = "nhmrc_grant_id";
	private static final String FIELD_APPLICATION_YEAR = "application_year";
	private static final String FIELD_SUB_TYPE = "sub_type";
	private static final String FIELD_HIGHER_GRANT_TYPE = "higher_grant_type";
	private static final String FIELD_SCIENTIFIC_TITLE = "scientific_title";
	private static final String FIELD_SIMPLIFIED_TITLE = "simplified_title";
	private static final String FIELD_CIA_NAME = "cia_name";
	private static final String FIELD_START_YEAR = "start_year";
	private static final String FIELD_END_YEAR = "end_year";
	private static final String FIELD_TOTAL_BUDGET = "total_budget";
	private static final String FIELD_RESEARCH_AREA = "research_area";
	private static final String FIELD_FOR_CATEGORY = "for_category";
	private static final String FIELD_OF_RESEARCH = "field_of_research";
	private static final String FIELD_KEYWORDS = "keywords";
	private static final String FIELD_HEALTH_KEYWORDS = "health_keywords";
	private static final String FIELD_MEDIA_SUMMARY = "media_summary";
	private static final String FIELD_SOURCE_SYSTEM = "source_system";
	
	private static final String FIELD_ROLE = "role";
	private static final String FIELD_DW_INDIVIDUAL_ID = "dw_individual_id";
	private static final String FIELD_SOURCE_INDIVIDUAL_ID = "source_individual_id";
	private static final String FIELD_TITLE = "title";
	private static final String FIELD_FIRST_NAME = "first_name";
	private static final String FIELD_MIDDLE_NAME = "middle_name";
	private static final String FIELD_LAST_NAME = "last_name";
	private static final String FIELD_FULL_NAME = "full_name";
	private static final String FIELD_ROLE_START_DATE = "role_start_date";
	private static final String FIELD_ROLE_END_DATE = "role_end_date";
	
    private static enum RelTypes implements RelationshipType
    {
        AdminInstitute, Investigator, KnownAs
    }

    private static enum Labels implements Label {
    	NHMRC, RDA, Institution, Grant, Researcher
    };

    private Index<Node> indexNHMRCGrant;
	private Index<Node> indexNHMRCResearcher;
	private Index<Node> indexNHMRCInstitution;
	
	private GraphDatabaseService graphDb;
	
	public Importer(final String neo4jFolder) {
		graphDb = GraphUtils.getGraphDb(neo4jFolder);
	
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			GraphUtils.createConstrant(graphDb, LABEL_NHMRC_GRANT, GraphUtils.PROPERTY_KEY);
			GraphUtils.createConstrant(graphDb, LABEL_NHMRC_RESEARCHER, GraphUtils.PROPERTY_KEY);
			GraphUtils.createConstrant(graphDb, LABEL_NHMRC_INSTITUTION, GraphUtils.PROPERTY_KEY);
		
			tx.success();
		}
		
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			indexNHMRCGrant = GraphUtils.getNodeIndex(graphDb, LABEL_NHMRC_GRANT);
			indexNHMRCResearcher = GraphUtils.getNodeIndex(graphDb, LABEL_NHMRC_RESEARCHER);
			indexNHMRCInstitution = GraphUtils.getNodeIndex(graphDb, LABEL_NHMRC_INSTITUTION);
		
			tx.success();
		}
	}
	
	public void importGrants()
	{
		// Imoprt Grant data
		System.out.println("Importing Grant data");
		long grantsCounter = 0;
	//	long transactionCount = 0;
		long beginTime = System.currentTimeMillis();
		
		// process grats data file
		CSVReader reader;
		try 
		{
			//Transaction tx = graphDb.beginTx(); 
			
			reader = new CSVReader(new FileReader(GRANTS_CSV_PATH));
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 57)
					continue;
				
				/*
				if (transactionCount > MAX_REQUEST_PER_TRANSACTION)
				{
					tx.success();  
					tx.finish(); 
					tx = graphDb.beginTx(); 
				}*/
				
				int grantId = Integer.parseInt(grant[0]);
				System.out.println("Grant id: " + grantId);			
				
				try ( Transaction tx = graphDb.beginTx() ) 
				{
					String purl = "http://purl.org/au-research/grants/nhmrc/" + grantId;

					Map<String, Object> map = new HashMap<String, Object>();
					
					map.put(FIELD_PURL, purl);
					map.put(FIELD_NHMRC_GRANT_ID, grantId);					
					map.put(FIELD_APPLICATION_YEAR, Integer.parseInt(grant[1]));
					map.put(FIELD_SUB_TYPE, grant[2]);
					map.put(FIELD_HIGHER_GRANT_TYPE, grant[3]);					
					map.put(FIELD_SCIENTIFIC_TITLE, grant[9]);
					map.put(FIELD_SIMPLIFIED_TITLE, grant[10]);
					map.put(FIELD_CIA_NAME, grant[11]);
					map.put(FIELD_START_YEAR, Integer.parseInt(grant[12]));
					map.put(FIELD_END_YEAR, Integer.parseInt(grant[13]));
					map.put(FIELD_TOTAL_BUDGET, grant[41]);
					map.put(FIELD_RESEARCH_AREA, grant[42]);
					map.put(FIELD_FOR_CATEGORY, grant[43]);
					map.put(FIELD_OF_RESEARCH, grant[44]);
					map.put(FIELD_MEDIA_SUMMARY, grant[54]);
					map.put(FIELD_SOURCE_SYSTEM, grant[55]);
					
				/*	List<String> keywords = null;
					for (int i = 45; i <= 49; ++i)
						if (grant[i].length() > 0)
						{
							if (null == keywords)
								keywords = new ArrayList<String>();
							keywords.add(grant[i]);
						}
					
					if (null != keywords)
						map.put(FIELD_KEYWORDS, keywords.toArray(new String[keywords.size()]) );
				
					keywords = null; 
					for (int i = 50; i <= 54; ++i)
						if (grant[i].length() > 0)
						{
							if (null == keywords)
								keywords = new ArrayList<String>();
							keywords.add(grant[i]);
						}
					
					if (null != keywords)
						map.put(FIELD_HEALTH_KEYWORDS, keywords.toArray(new String[keywords.size()]) );*/
				

					Node nodeInstitution = getOrCreateNHMRCInstitution(grant[6], grant[7], grant[8]);
					Node nodeGrant = GraphUtils.createUniqueNode(graphDb, indexNHMRCGrant, purl, 
							Labels.NHMRC, Labels.Grant, map);
					GraphUtils.createUniqueRelationship(nodeGrant, nodeInstitution, 
							RelTypes.AdminInstitute, Direction.OUTGOING, null);
					
					tx.success();
					
				}

				++grantsCounter;
			}
			
		/*	tx.success();  
			tx.finish(); */
	
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			
			return;
		}
		
		long endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grants over %d ms. Average %f ms per grant", 
				grantsCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)grantsCounter));
	
		
		long granteesCounter = 0;
		beginTime = System.currentTimeMillis();
	
		try 
		{
			reader = new CSVReader(new FileReader(ROLES_CSV_PATH));
			String[] grantee;
			boolean header = false;
			while ((grantee = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grantee.length != 12)
					continue;
				
			/*	Map<String, Object> par = new HashMap<String, Object>();
				par.put(FIELD_GRANT_ID, Integer.parseInt(grantee[0]));*/
				
				int grantId = Integer.parseInt(grantee[0]);
				String dwIndividualId = grantee[2];
				System.out.println("Investigator id: " + dwIndividualId);	
				
				try ( Transaction tx = graphDb.beginTx() ) 
				{
					Map<String, Object> map = new HashMap<String, Object>();
				//	map.put(FIELD_KEY, dwIndividualId);
					map.put(FIELD_DW_INDIVIDUAL_ID, dwIndividualId);
					map.put(FIELD_SOURCE_INDIVIDUAL_ID, grantee[3]);
					map.put(FIELD_TITLE, grantee[4]);
					map.put(FIELD_FIRST_NAME, grantee[5]);
					map.put(FIELD_MIDDLE_NAME, grantee[6]);
					map.put(FIELD_LAST_NAME, grantee[7]);
					map.put(FIELD_FULL_NAME, grantee[8]);
					map.put(FIELD_SOURCE_SYSTEM, grantee[11]);
					
					Node nodeGrantee = GraphUtils.createUniqueNode(graphDb, indexNHMRCResearcher, dwIndividualId, 
							Labels.NHMRC, Labels.Researcher, map);
					
					String purl = "http://purl.org/au-research/grants/nhmrc/" + grantId;
					
					Node nodeGrant = GraphUtils.findNode(indexNHMRCGrant, purl);
					if (nodeGrant != null) 
					{
						map = new HashMap<String, Object>();
						map.put(FIELD_ROLE, grantee[1]);
						map.put(FIELD_ROLE_START_DATE, grantee[9]);
						map.put(FIELD_ROLE_END_DATE, grantee[10]);
						
						GraphUtils.createUniqueRelationship(nodeGrantee, nodeGrant,
								RelTypes.AdminInstitute, Direction.OUTGOING, null);
					}
					
					tx.success();
				}
				
				++granteesCounter;
			}
	
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			
			return;
		}
		
		endTime = System.currentTimeMillis();
		
		System.out.println(String.format("Done. Imporded %d grantees and create relationships over %d ms. Average %f ms per grantee", 
				granteesCounter, endTime - beginTime, (float)(endTime - beginTime) / (float)granteesCounter));
	}
	
	private Node getOrCreateNHMRCInstitution(String name, String state, String type) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(FIELD_NAME, name);
		map.put(FIELD_STATE, state);
		map.put(FIELD_TYPE, type);
	
		return GraphUtils.createUniqueNode(graphDb, indexNHMRCInstitution, name, 
				Labels.NHMRC, Labels.Institution, map);
	}
	
}
