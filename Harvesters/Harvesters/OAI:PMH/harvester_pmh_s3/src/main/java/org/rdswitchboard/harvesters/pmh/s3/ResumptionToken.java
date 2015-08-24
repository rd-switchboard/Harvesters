package org.rdswitchboard.harvesters.pmh.s3;

public class ResumptionToken {
	private String token = null;
	private Integer cursor = null;
	private Integer size = null;
	
	public String getToken() {
		return token;
	}
	
	public void setToken(String token) {
		this.token = token;
	}
	
	public Integer getCursor() {
		return cursor;
	}
	
	public void setCursor(Integer cursor) {
		this.cursor = cursor;
	}
	
	public Integer getSize() {
		return size;
	}
	
	public void setSize(Integer size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return "ResumptionToken [token=" + token + ", cursor=" + cursor
				+ ", size=" + size + "]";
	}
}
