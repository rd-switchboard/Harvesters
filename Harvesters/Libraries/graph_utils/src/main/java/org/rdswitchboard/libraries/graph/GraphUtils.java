package org.rdswitchboard.libraries.graph;

public class GraphUtils {
	// schema fields
	public static final String SCHEMA_LABEL = "label";
	public static final String SCHEMA_INDEX = "index";
	public static final String SCHEMA_UNIQUE = "unique";
	
	// required properties
	public static final String PROPERTY_KEY = "key";
	public static final String PROPERTY_SOURCE = "node_source";
	public static final String PROPERTY_TYPE = "node_type";
	public static final String PROPERTY_TITLE = "title";
	public static final String PROPERTY_RDS_URL = "rds_url";
	
	// properties required for meta-data harmonization  
	public static final String PROPERTY_URL = "url"; 
   
	// optional properties
	public static final String PROPERTY_NLA = "nla";
	public static final String PROPERTY_DOI = "doi";
	public static final String PROPERTY_ORCID = "orcid";
	public static final String PROPERTY_PURL = "purl";
	public static final String PROPERTY_LOCAL_ID = "local_id";
	public static final String PROPERTY_NAME_PREFIX = "name_prefix";
	public static final String PROPERTY_FIRST_NAME = "first_name";
	public static final String PROPERTY_MIDDLE_NAME = "middle_name";
	public static final String PROPERTY_LAST_NAME = "last_name";
	public static final String PROPERTY_FULL_NAME = "full_name";
	public static final String PROPERTY_COUNTRY = "country";
	public static final String PROPERTY_STATE = "state";
	public static final String PROPERTY_HOST = "host";
	public static final String PROPERTY_PATTERN = "pattern";
	public static final String PROPERTY_ORIGINAL_SOURCE = "original_source";
	public static final String PROPERTY_CONTRIBUTORS = "contributors";
	public static final String PROPERTY_SCOPUS_ID = "scopus_id";
	public static final String PROPERTY_ORCID_ID = "orcid_id";
	public static final String PROPERTY_ANDS_GROUP = "ands_group";
	public static final String PROPERTY_AWARDED_DATE = "awarded_date";
	public static final String PROPERTY_PUBLISHED_DATE = "published_date";
	public static final String PROPERTY_ARC_ID = "arc_id";
	public static final String PROPERTY_NHMRC_ID = "nhmrc_id";
	
	// control properties
	public static final String PROPERTY_DELETED = "deleted";
	public static final String PROPERTY_BROKEN = "broken";
	
	// meta-data sources
	public static final String SOURCE_ANDS = "ands";
	public static final String SOURCE_ARC = "arc";
	public static final String SOURCE_NHMRC = "nhmrc";
	public static final String SOURCE_WEB = "web";
	public static final String SOURCE_ORCID = "orcid";
	
	// meta-data types
	public static final String TYPE_DATASET = "dataset";
	public static final String TYPE_GRANT = "grant";
	public static final String TYPE_RESEARCHER = "researcher";
	public static final String TYPE_INSTITUTION = "institution";
	public static final String TYPE_SERVICE = "service";
	public static final String TYPE_PUBLICATION = "publication";
	
	// relationships
	
	public static final String RELATIONSHIP_RELATED_TO = "relatedTo";
	public static final String RELATIONSHIP_KNOWN_AS = "knownAs";
	public static final String RELATIONSHIP_AUTHOR = "author";	
}
