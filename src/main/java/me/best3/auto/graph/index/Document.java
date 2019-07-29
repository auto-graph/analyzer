package me.best3.auto.graph.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class Document implements Iterable<IndexableField> {
	public static final Logger logger = LogManager.getLogger(Document.class);
	
	private org.apache.lucene.document.Document document = null;

	public Document() {
		this.document = new org.apache.lucene.document.Document();
	}
	
	public Document(org.apache.lucene.document.Document document) {
		this.document = document;
	}
	
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

	Query getAllFieldsMatchQuery() {
		if(logger.isDebugEnabled()) {
			logger.debug("All fields query called.");
		}
		List<IndexableField> fields = this.document.getFields();
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
			fields.stream().forEach(field -> {
				booleanQueryBuilder.add(new BooleanClause(new TermQuery(new Term(field.name(), this.document.get(field.name()))), BooleanClause.Occur.MUST));
			});			
			return booleanQueryBuilder.build();
	}
	org.apache.lucene.document.Document getDocument(){
		return this.document;
	}
}
