package me.best3.auto.graph.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;

public class Document implements Iterable<IndexableField> {
	private org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();

	@Override
	public Iterator<IndexableField> iterator() {
		return this.document.iterator();
	}
	
	public void addString(String fieldName, String value) {
		addFieldToDocument(new StringField(fieldName, value, Store.YES));
	}
	
	public void addText(String fieldName, String value) {
		addFieldToDocument(new TextField(fieldName,value,Store.YES));
	}
	
	private void addFieldToDocument(Field field) {
		this.document.add(field);
	}
	
	public String get(String fieldName) {
		return this.document.get(fieldName);
	}
	
	public void RemoveField(String fieldName) {
		this.document.removeField(fieldName);
	}
	
	public void RemoveFields(String fieldName) {
		this.document.removeFields(fieldName);
	}
	
	public List<String> getFields(){
		List<String> fields = this.document.getFields()
				.parallelStream()
				.map((f) -> { return f.name();})
				.collect(Collectors.toList());
		Collections.sort(fields);
		return fields;
	}

	org.apache.lucene.document.Document getDocument(){
		return this.document;
	}
}
