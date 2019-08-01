package me.best3.auto.graph.analyzer;

import java.util.Arrays;
import java.util.Comparator;

import me.best3.auto.graph.index.Document;

/**
 * A rough natural ordering of sets with subsets on top and supersets below with,
 * interspersed by intersecting sets
 * 
 * @author spottur2
 *
 */
public class SubsetComparator implements Comparator<Document> {

	@Override
	public int compare(Document docA, Document docB) {
		String[] docAFields = docA.getFields().toArray(new String[0]);
		String[] docBFields = docB.getFields().toArray(new String[0]);
		/*
		 * Finds and returns the index of the first mismatch between two Object arrays, 
		 * otherwise return -1 if no mismatch is found. The index will be in the range 
		 * of 0 (inclusive) up to the length (inclusive) of the smaller array.
		 * */
		int mismatchIndex =  Arrays.mismatch(docAFields,docBFields,Comparator.naturalOrder());
		if(mismatchIndex == -1) {//Equivalent sets, also implies both are subset of the other
			return 0;
		}else if(mismatchIndex == 0 && docAFields.length > docBFields.length) { // no intersection at all, just go by field length
			return 1;
		}else if(mismatchIndex > 0 && docAFields.length > docBFields.length) {//sets intersect			
			return 1;
		}else {
			return -1;
		}
	}
	
	public static boolean intersects(Document docA, Document docB) {
		String[] docAFields = docA.getFields().toArray(new String[0]);
		String[] docBFields = docB.getFields().toArray(new String[0]);
		/*
		 * Finds and returns the index of the first mismatch between two Object arrays, 
		 * otherwise return -1 if no mismatch is found. The index will be in the range 
		 * of 0 (inclusive) up to the length (inclusive) of the smaller array.
		 * */
		int mismatchIndex =  Arrays.mismatch(docAFields,docBFields,Comparator.naturalOrder());
		
		if(mismatchIndex == -1 || mismatchIndex > 0) {
			return true;
		}else /*if(mismatchIndex == 0)*/ {
			return false;
		}
	}

}
