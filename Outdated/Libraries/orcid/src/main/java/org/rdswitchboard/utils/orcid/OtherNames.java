package org.rdswitchboard.utils.orcid;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

public class OtherNames {
	private List<String> names;
	private String visibility;
	
	@JsonProperty("other-name")
	public List<String> getNames() {
		return names;
	}
	
	@JsonProperty("other-name")
	@JsonDeserialize(using = ValueDeserializer.class)
	public void setNames(List<String> names) {
		this.names = names;
	}
	
	public String getVisibility() {
		return visibility;
	}
	
	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}
	
	@Override
	public String toString() {
		return "OtherNames [names=" + names + ", visibility="
				+ visibility + "]";
	}	
}
