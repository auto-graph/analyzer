package me.best3.auto.graph.analyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import me.best3.auto.graph.index.Document;

/**
 * A rough natural ordering of sets with subsets on top and supersets below with,
 * interspersed by intersecting sets
 * 
 * @author spottur2
 *
 */
public class SubsetComparator implements Comparator<Document> {
	private static final Logger logger = LogManager.getLogger(SubsetComparator.class); 
	@Override
	public int compare(Document docA, Document docB) {
		List<String> docAFields = docA.getFields();
		List<String> docBFields = docB.getFields();
		if(docAFields.size()==0) {
			return -1;
		}
		List<String> commonFields = docAFields.parallelStream().filter(docAField -> docBFields.contains(docAField)).collect(Collectors.toList());
		if(commonFields.size() == docAFields.size() && docAFields.size() == docBFields.size()) {//A and B have same fields
			return 0;
		}else if(docBFields.size()>0 && (docAFields.size()>docBFields.size())) {
			return 1;
		}else if(commonFields.size() == docAFields.size() && docAFields.size() < docBFields.size()) {//A is a subset of B
			return -1;
		}
		return -1;
	}
	
	public static boolean intersects(Document docA, Document docB) {
		if(logger.isDebugEnabled()) {
			try {
				logger.debug(String.format("Doc A : %s\nDoc B : %s", docA.toJSON(),docB.toJSON()));
			} catch (IOException e) {
				logger.error(e,e);
			}
		}
		List<String> docAFields = docA.getFields();
		List<String> docBFields = docB.getFields();
		if(docAFields.size()==0) {
			if(logger.isDebugEnabled()) {
				logger.debug("DocA is a null set and its has no intersection with any non null set.");
			}
			return false;
		}
		if(docAFields.size()==0 && docBFields.size()==0) {
			if(logger.isDebugEnabled()) {
				logger.debug("DocA is a null set and so is DocB they interset.");
			}
			return true;
		}
		List<String> commonFields = docAFields.parallelStream().filter(docAField -> docBFields.contains(docAField)).collect(Collectors.toList());
		return (commonFields.size() >0);
	}
	
	public static boolean isSubSet(Document docA, Document docB) {
		if(logger.isDebugEnabled()) {
			try {
				logger.debug(String.format("Doc A : %s\nDoc B : %s", docA.toJSON(),docB.toJSON()));
			} catch (IOException e) {
				logger.error(e,e);
			}
		}
		List<String> docAFields = docA.getFields();
		List<String> docBFields = docB.getFields();
		if(docAFields.size()==0) {
			if(logger.isDebugEnabled()) {
				logger.debug("DocA is a null set and its a subset of all sets.");
			}
			return true;
		}
		List<String> commonFields = docAFields.parallelStream().filter(docAField -> docBFields.contains(docAField)).collect(Collectors.toList());
		if(logger.isDebugEnabled()) {
			logger.debug(String.format("Doc A subset of Doc B : %s", (commonFields.size() == docAFields.size() )));
		}
		return (commonFields.size() == docAFields.size() );
	}

}
