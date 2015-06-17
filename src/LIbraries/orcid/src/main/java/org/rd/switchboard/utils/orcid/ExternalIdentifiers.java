package org.rd.switchboard.utils.orcid;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class ExternalIdentifiers {
	private List<ExternalIdentifier> identifiers;
	private String visibility;
	
	@JsonProperty("external-identifier")
	public List<ExternalIdentifier> getIdentifiers() {
		return identifiers;
	}

	@JsonProperty("external-identifier")
	public void setIdentifiers(List<ExternalIdentifier> identifiers) {
		this.identifiers = identifiers;
	}
	
	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}


	@Override
	public String toString() {
		return "ExternalIdentifiers [identifiers=" + identifiers + ", visibility=" + visibility + "]";
	}
}
