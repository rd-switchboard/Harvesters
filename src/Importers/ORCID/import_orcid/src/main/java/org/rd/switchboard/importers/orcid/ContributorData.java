package org.rd.switchboard.importers.orcid;

public class ContributorData {
	private String researcherKey;
	private long workId;
	private String sequince;
	private String role;
	private String name;
	
	public String getResearcherKey() {
		return researcherKey;
	}
	
	public void setResearcherKey(String researcherKey) {
		this.researcherKey = researcherKey;
	}
	
	public long getWorkId() {
		return workId;
	}
	
	public void setWorkId(long workId) {
		this.workId = workId;
	}
	
	public String getSequince() {
		return sequince;
	}
	
	public void setSequince(String sequince) {
		this.sequince = sequince;
	}
	
	public String getRole() {
		return role;
	}
	
	public void setRole(String role) {
		this.role = role;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		return "ContributorData [researcherKey=" + researcherKey 
				+ ", workId=" + workId 
				+ ", sequince=" + sequince 
				+ ", role=" + role 
				+ ", name=" + name + "]";
	}
}
