package org.rdswitchboard.harvesters.pmh.s3;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Class to store information about supported metadata
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class MetadataFormat {
	
	protected static final String TAG_METADATA_FORMAT = "metadataFormat";
	protected static final String TAG_METADATA_PREFIX = "metadataPrefix";
	protected static final String TAG_SCHEMA = "schema";
	protected static final String TAG_METADATA_NAMESPACE = "metadataNamespace";
	
	private String metadataPrefix;
	private String schema;
	private String metadataNamespace;
	
	/**
	 * Return metadata prefix as string
	 * @return String - metadata prefix
	 */
	public String getMetadataPrefixString() {
		return metadataPrefix;
	}

	/**
	 * Function will try to convert string containing metadata prefix into actual MetadataPrefix
	 * @return MetadataPrefix
	 */
	public String getMetadataPrefix() {
		return metadataPrefix;
	}
	
	/**
	 * Set metadata prefix
	 * @param metadataPrefix an metadata prefix string
	 */
	public void setMetadataPrefixString(String metadataPrefix) {
		this.metadataPrefix = metadataPrefix;
	}

	/**
	 * Return schema
	 * @return String - schema
	 */
	public String getSchema() {
		return schema;
	}
	
	/**
	 * Set Schema
	 * @param schema String
	 */
	public void setSchema(String schema) {
		this.schema = schema;
	}

	/**
	 * Return metatdata namespace
	 * @return String - metadata namespace
	 */
	public String getMetadataNamespace() {
		return metadataNamespace;
	}

	/**
	 * Set metadata namespace 
	 * @param metadataNamespace Sring
	 */
	public void setMetadataNamespace(String metadataNamespace) {
		this.metadataNamespace = metadataNamespace;
	}
	
	/**
	 * Construct MetadataFormat object from XML Element
	 * @param element Element
	 * @return MetadataFormat
	 */
	public static MetadataFormat fromElement(Element element) {
		MetadataFormat metadataFormat = new MetadataFormat();
		
		for(Node child = element.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element) {
	        	String tagName = ((Element) child).getTagName();
	        	if (null != tagName) {
	        		if (tagName.equals(TAG_METADATA_PREFIX)) 
	        			metadataFormat.metadataPrefix = child.getTextContent();
	        		else if (tagName.equals(TAG_SCHEMA)) 
	        			metadataFormat.schema = child.getTextContent();
	        		else if (tagName.equals(TAG_METADATA_NAMESPACE)) 
	        			metadataFormat.metadataNamespace = child.getTextContent();
	        	}
	        }	
	
		return metadataFormat;
	}
	
	/**
	 * Get all metadata format elements from the Document
	 * @param doc XML Document
	 * @return {@code List<MetadataFormat>}
	 */
	public static List<MetadataFormat> getMetadataFormats(Document doc) {
		List<MetadataFormat> metadataFormats = new ArrayList<MetadataFormat>();
		
		NodeList list = doc.getElementsByTagName(TAG_METADATA_FORMAT);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element) 
					metadataFormats.add(fromElement((Element) node));
					
			}
		
		return metadataFormats;		
	}
	
	/**
	 * Converts object to string
	 * @return String
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(TAG_METADATA_FORMAT);
		sb.append(": [");
		sb.append(TAG_METADATA_PREFIX);
		sb.append("=");
		sb.append(metadataPrefix);
		sb.append(", ");
		sb.append(TAG_SCHEMA);
		sb.append("=");
		sb.append(schema);
		sb.append(", ");
		sb.append(TAG_METADATA_NAMESPACE);
		sb.append("=");
		sb.append(metadataNamespace);
		sb.append("]");
		
		return sb.toString();
	}
}
