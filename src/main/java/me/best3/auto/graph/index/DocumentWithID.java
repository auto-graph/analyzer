package me.best3.auto.graph.index;

import java.util.UUID;

public class DocumentWithID extends Document {
	public static final String ID_FIELD = "ID";
	
	public DocumentWithID(org.apache.lucene.document.Document doc) {
		super(doc);
		//because documents are serialized and pulled back out this field might have been already in there 
		if(doc.getFields(ID_FIELD).length<=0) {
			generateID();
		}
	}
	
	public DocumentWithID(Document doc) {
		this(doc.getDocument());
	}
	
	public DocumentWithID() {
		super();
		generateID();
	}

	private void generateID() {
		String uuid = UUID.randomUUID().toString();
		addString(ID_FIELD, uuid);
	}
	
	public String[] getExcludeFields() {
		return new String[] {ID_FIELD};
	}

	
	@Override
	public int hashCode() {
		return get(ID_FIELD).hashCode();
	}
	

	@Override
	public boolean equals(Object doc) {
		if(doc instanceof DocumentWithID) {
			return get(ID_FIELD).equals(((DocumentWithID)doc).get(ID_FIELD));
		}
		return false;
	}
	
	
}
