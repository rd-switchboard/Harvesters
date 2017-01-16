package org.rdswitchboard.harvesters.pmh;

public class HarvesterException extends Exception {

	private String code;

	public HarvesterException(String message) {
		super(message);
	}

	public HarvesterException(String code, String message) {
		super(message);

		this.code = code;
	}
	
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Override
	public String getMessage() {
		if (null == code || code.isEmpty())
			return super.getMessage();
		else
			return String.format("[ %s ] %s", code, super.getMessage());
	}
}
