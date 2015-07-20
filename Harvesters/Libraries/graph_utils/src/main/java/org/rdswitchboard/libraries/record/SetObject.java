package org.rdswitchboard.libraries.record;

import java.util.HashSet;

public class SetObject extends HashSet<Object>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2678497257372998171L;

	public Object getAny() {
		return iterator().next();
	}
	
	public Class<?> getStoredClass() {
		if (isEmpty())
			return null;
		return getAny().getClass();			
	}
	
	@Override
	public boolean add(Object value) {
		// if set is not empty
		if (!isEmpty()) {
			Class<?> clss = getAny().getClass();
			// check what new element class is same, as element of any object, already stored in the set
			if (!clss.equals(value.getClass()))
				throw new ClassCastException("The class " + value.getClass() + " is not the same as " + clss);
		}
		
		return super.add(value);
	}
}