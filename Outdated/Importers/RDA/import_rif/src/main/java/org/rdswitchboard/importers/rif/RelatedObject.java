package org.rdswitchboard.importers.rif;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to store a RDA relation
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class RelatedObject {
	public String key;
	public String relatedKey;
	public String relationshipName;
	public List<String> url;
	public List<String> description;
	
	public void addDescription(final String description) {
		if (null != description && !description.isEmpty()) {
			if (null == this.description)
				this.description = new ArrayList<String>();
			
			this.description.add(description);
		}			
	}
	
	public void addUrl(final String url) {
		if (null != url && !url.isEmpty()) {
			if (null == this.url)
				this.url = new ArrayList<String>();
			
			this.url.add(url);
		}		
	}
}
