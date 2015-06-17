package org.rd.switchboard.utils.orcid;

import java.util.List;

public class ContactDetails {
	private List<String> email;
	private Address address;

	public List<String> getEmail() {
		return email;
	}

	public void setEmail(List<String> email) {
		this.email = email;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return "ContactDetails [email=" + email + ", address=" + address + "]";
	}
}
