package org.rdswitchboard.utils.orcid;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

public class ResearcherUrl {
	private String name;
	private String url;
	
	@JsonProperty("url-name")
	public String getName() {
		return name;
	}
	
	@JsonProperty("url-name")
	@JsonDeserialize(using = ValueDeserializer.class)
	public void setName(String name) {
		this.name = name;
	}
	
	public String getUrl() {
		return url;
	}
	
	@JsonDeserialize(using = ValueDeserializer.class)
	public void setUrl(String url) {
		this.url = url;
	}
	
	@Override
	public String toString() {
		return "ResearcherUrl [name=" + name + ", url=" + url + "]";
	}
	
	
	
}
