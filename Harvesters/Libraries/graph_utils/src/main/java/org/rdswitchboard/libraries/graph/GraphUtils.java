package org.rdswitchboard.libraries.graph;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

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
	//public static final String PROPERTY_ORCID = "orcid";
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
	public static final String PROPERTY_ORCID_ID = "orcid";
	public static final String PROPERTY_ANDS_GROUP = "ands_group";
	public static final String PROPERTY_AWARDED_DATE = "awarded_date";
	public static final String PROPERTY_PUBLISHED_DATE = "published_date";
	public static final String PROPERTY_ARC_ID = "arc_id";
	public static final String PROPERTY_NHMRC_ID = "nhmrc_id";
	public static final String PROPERTY_ISBN = "isbn";
	public static final String PROPERTY_ISSN = "issn";
	
	// control properties
	public static final String PROPERTY_DELETED = "deleted";
	public static final String PROPERTY_BROKEN = "broken";
	
	// meta-data sources
	public static final String SOURCE_ANDS = "ands";
	public static final String SOURCE_ARC = "arc";
	public static final String SOURCE_NHMRC = "nhmrc";
	public static final String SOURCE_WEB = "web";
	public static final String SOURCE_ORCID = "orcid";
	public static final String SOURCE_DRYAD = "dryad";
	public static final String SOURCE_CROSSREF = "crossref";
	public static final String SOURCE_FIGSHARE = "figshare";
	// meta-data types
	public static final String TYPE_DATASET = "dataset";
	public static final String TYPE_GRANT = "grant";
	public static final String TYPE_RESEARCHER = "researcher";
	public static final String TYPE_INSTITUTION = "institution";
	public static final String TYPE_SERVICE = "service";
	public static final String TYPE_PUBLICATION = "publication";
	public static final String TYPE_PATTERN = "pattern";
	
	// relationships
	
	public static final String RELATIONSHIP_RELATED_TO = "relatedTo";
	public static final String RELATIONSHIP_KNOWN_AS = "knownAs";
	public static final String RELATIONSHIP_AUTHOR = "author";
	public static final String RELATIONSHIP_PATTERN = "pattern";
	
	public static final String SCOPUS_PARTNER_ID = "MN8TOARS";
	
	private static final String URL_REGEX = "^((https?|ftp)://|(www|ftp)\\.)?[a-z0-9-]+(\\.[a-z0-9-]+)+([/?].*)?$";
	private static final String DOI_REGEX = "\\d{2}\\.\\d{4,}/.+$";
	private static final String ORCID_REGEX = "\\d{4}-\\d{4}-\\d{4}-\\d{3}(\\d|X)";
	private static final String SCOPUS_AUTHOR_REGEX = "author[iI][dD]=\\d+";
	private static final String SCOPUS_PARTNER_REGEX = "partner[iI][dD]=[A-Z0-9]+";
	
    private static final String PART_PROTOCOL = "://";
    private static final String PART_SLASH = "/";
    private static final String PART_EQUALS = "=";
    private static final String PART_WWW = "www.";
    private static final String PART_ORCID_URI = "orcid.org/";
    //private static final String PART_DOI_PERFIX = "doi:";
    private static final String PART_DOI_URI = "dx.doi.org/";
    private static final String PART_SCOPUS_URL = "www.scopus.com/inward/authorDetails.url?authorID=%s&partnerID=%s";
    
    private static final Pattern patternUrl = Pattern.compile(URL_REGEX);
    private static final Pattern patternDoi = Pattern.compile(DOI_REGEX);
    private static final Pattern patternOrcid = Pattern.compile(ORCID_REGEX);
    private static final Pattern patternScopusAuthor = Pattern.compile(SCOPUS_AUTHOR_REGEX);
    private static final Pattern patternScopusPartner = Pattern.compile(SCOPUS_PARTNER_REGEX);
    
    
    
	/**
	 * Function to check the string and extract any URL from it
	 * 
	 * @param str
	 * @return URL String
	 */    
	
	public static String extractUrl(String str) {
		if (StringUtils.isNotEmpty(str)) {
    		Matcher matcher = patternUrl.matcher(str);
			if (matcher.find())
				return matcher.group();
    	}
    	
    	return null;
	}
	
	/**
	 * Function to extract formalized URL
	 * 
	 * The formalized url does not have protocol, www or ftp part of the host name and the terminating slash 
	 *  
	 * @param str : URL String
	 * @return Standardized URL String
	 * @throws MalformedURLException 
	 */
	
	public static String extractFormalizedUrl(String str) throws MalformedURLException {
		if (StringUtils.isNotEmpty(str)) {
       		// make sure URL contains a protocol
			URL url = new URL(str.indexOf( PART_PROTOCOL ) >= 0 ? str : "http://" + str);

			// extract host name
   			String host = url.getHost();
   			
   			// cut of www. from host name
   			if (host.startsWith(PART_WWW))
   				host = host.substring(PART_WWW.length());
	    
   			// extract file name
   			String file = url.getFile();
   			
   			// cut of terminating slash from a file name
   			if (file.endsWith(PART_SLASH))
   				file = file.substring(0, file.length()-1);
   			
   			// return extracted url
   			return host + file;
    	}
    	
    	return null;
    }
	
	public static String extractFormalizedUrlSafe(String str) {
		try {
			return extractFormalizedUrl(str);
		} catch (MalformedURLException e) {
			e.printStackTrace();

			return null;
		}
    }
	
	public static String extractOrcidId(String str) {
    	if (StringUtils.isNotEmpty(str)) {
    		Matcher matcher = patternOrcid.matcher(str);
			if (matcher.find())
				return matcher.group();
    	}
    	
    	return null;
	}
	
	public static String extractDoi(String str) {
    	if (StringUtils.isNotEmpty(str)) {
    		Matcher matcher = patternDoi.matcher(str);
    		if (matcher.find()) 
    			return matcher.group();
    	}
    	
		return null;
	}
		
	public static String extractScopusAuthorId(String str) {
    	if (StringUtils.isNotEmpty(str)) {
    		Matcher matcher = patternScopusAuthor.matcher(str);
    		if (matcher.find()) {
    			String scopus =  matcher.group();
    			int pos = scopus.indexOf(PART_EQUALS);
    			if (pos >= 0) {
    				scopus = scopus.substring(pos + PART_EQUALS.length());
    				if (!scopus.isEmpty())
    					return scopus;
    			}
    		}
    	}
    	
		return null;
	}
	
	public static String extractScopusPartnerId(String str) {
    	if (StringUtils.isNotEmpty(str)) {
    		Matcher matcher = patternScopusPartner.matcher(str);
    		if (matcher.find()) {
    			String scopus =  matcher.group();
    			int pos = scopus.indexOf(PART_EQUALS);
    			if (pos >= 0) {
    				scopus = scopus.substring(pos + PART_EQUALS.length());
    				if (!scopus.isEmpty())
    					return scopus;
    			}
    		}
    	}
    	
		return null;
	}

	public static String generateOrcidUri(String orcid) {
    	return StringUtils.isEmpty(orcid) ? null : (PART_ORCID_URI + orcid);
    }

    public static String generateDoiUri(String doi) {
    	return StringUtils.isEmpty(doi) ? null : (PART_DOI_URI + doi);
    }

    public static String generateScopusUri(String authorId, String partnerId) {
    	return (StringUtils.isEmpty(authorId) || StringUtils.isEmpty(partnerId)) ? null : String.format(PART_SCOPUS_URL, authorId, partnerId);
    }
    
    public static String generateScopusUri(String authorId) {
    	return generateScopusUri(authorId, SCOPUS_PARTNER_ID);
    }
}
