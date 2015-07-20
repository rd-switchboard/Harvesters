package org.rdswitchboard.libraries.record;

import java.util.HashMap;

public class MapSet extends HashMap<String, Object>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8736089511042701173L;

	public void add(String key, Object value) {
		if (null != value) {
			Object par = get(key);
			if (null == par) 
				put(key, value);
			else {
				if (par instanceof SetObject) {
					((SetObject) par).add(value);
				} else {
					if (par.equals(value))
						return; // nothing to do
					SetObject set = new SetObject();
					set.add(par);
					set.add(value);
					put(key, set);
				}
			}
		}
	}
}
