package me.best3.auto.graph.analyzer;

import java.util.Arrays;
import java.util.Comparator;

import me.best3.auto.graph.index.Document;

public class SubsetComparator implements Comparator<Document> {

	@Override
	public int compare(Document docA, Document docB) {
		if(docA==null && docB==null) {
			return 0;
		}else if(docA==null && docB!=null) { // in our case a null set is a sub set of every non null set
			return 1; //B is superset of A (A U B)
		}else if (docA!=null && docB==null) {
			return -1; //A is superset of B (B U A)
		}
		String[] docAFields = docA.getFields().toArray(new String[0]);
		String[] docBFields = docB.getFields().toArray(new String[0]);
//		int smallerArrayLength = Math.min(docAFields.length, docBFields.length);
		/*
		 * Finds and returns the index of the first mismatch between two Object arrays, 
		 * otherwise return -1 if no mismatch is found. The index will be in the range 
		 * of 0 (inclusive) up to the length (inclusive) of the smaller array.
		 * */
		int mismatchIndex =  Arrays.mismatch(docAFields,docBFields,new Comparator<String>() {

			@Override
			public int compare(String docAField, String docBField) {
				return docAField.compareTo(docBField);
			}
		});
		
		if(mismatchIndex == -1 && docAFields.length == docBFields.length) {
			return 0;
		}else if(mismatchIndex == -1 && docBFields.length > docAFields.length) {//B is superset of A (A U B) , because this logically A<B
			return -1;
		}else if(mismatchIndex == -1 && docAFields.length > docBFields.length) {//A is superset of B (B U A) , because this is logically A>B
			return 1;
		}
		return 1;
	}

}
