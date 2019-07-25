package me.best3.auto.graph.analyzer;

import java.util.Arrays;
import java.util.Comparator;

import me.best3.auto.graph.index.Document;

public class SubsetComparitor implements Comparator<Document> {

	@Override
	public int compare(Document docA, Document docB) {
		String[] docAFields = (String[])docA.getFields().toArray();
		String[] docBFields = (String[])docB.getFields().toArray();
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
		
		if(mismatchIndex == -1) {
			return 0;
		}else if(mismatchIndex >= docAFields.length) {//B is superset of A (B U A)
			return -1;
		}else if(mismatchIndex >= docBFields.length) {//A is superset of B (A U B)
			return 1;
		}
		return 0;
	}

}
