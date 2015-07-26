package org.rdswitchboard.libraries.graph;


/**
 * A class to store schema of the Graph
 * 
 * Typically it has a label used to distinguish a Node by it source or type 
 * and an index name used to create index or constraint by that name.
 * Additional unique flag can be set to indicate what this index must be unique within a label.
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-24
 * @version 1.0.0
 */

public class GraphSchema extends GraphProperties {
	public GraphSchema() {
		
	}
	
	public GraphSchema(String label, String index, boolean unique) {
		
	}
	
	public String getLabel() {
		return (String) getProperty(GraphUtils.SCHEMA_LABEL);
	}
	
	public void setLabel(String label) {
		setProperty(GraphUtils.SCHEMA_LABEL, label);
	}
		 
	public GraphSchema withLabel(String label) {
		setLabel(label);
		return this;
	}
	
	public String getIndex() {
		return (String) getProperty(GraphUtils.SCHEMA_INDEX);
	}
	
	public void setIndex(String index) {
		setProperty(GraphUtils.SCHEMA_INDEX, index);
	}

	public GraphSchema withIndex(String index) {
		setIndex(index);
		return this;
	}
	 
	public boolean isUnique() {
		Boolean unique = (Boolean) getProperty(GraphUtils.SCHEMA_UNIQUE);
	    return null == unique ? false : unique; 
	}
	
	public void setUnique(boolean unique) {
		setProperty(GraphUtils.SCHEMA_UNIQUE, unique);
	}

	public GraphSchema withUnique(boolean unique) {
		setUnique(unique);
		return this;
	}
	
	/*@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof GraphSchema) 
			return this.unique == ((GraphSchema) obj).unique
					&& Objects.equals(this.label, ((GraphSchema) obj).label)
					&& Objects.equals(this.index, ((GraphSchema) obj).index);
		
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.unique, this.label, this.index);
	}*/

	@Override
	public String toString() {
		return "GraphSchema [properties=" + properties + "]";
	}
}
