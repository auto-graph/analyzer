package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LuceneDocumentIndexTest {
	private static final String FILE_INDEXER_TEST_INDEX = "./fileIndexerTestIndex/docindex";
	private static LuceneDocumentIndex docIndex;
	
	@BeforeAll
	public static void setup() {
		try {
			LuceneDocumentIndexTest.docIndex = new LuceneIndexFactory().getLuceneDocumentIndex(FILE_INDEXER_TEST_INDEX);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterAll
	public static void tearDown() {
		if(LuceneDocumentIndexTest.docIndex!=null) {
			try {
				LuceneDocumentIndexTest.docIndex.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testFindAll() {
		Document match = new Document();
		match.addString("fieldOne", "fieldOne");
		match.addString("fieldTwo", "fieldTwo");
//		match.addString("fieldThree", "fieldThree");
		try {			
			List<Document> results = LuceneDocumentIndexTest.docIndex.find(match);
			results.stream().map(d-> {try {return d.toJSON();} catch (IOException e) {e.printStackTrace();}return "";}).forEach(System.out::println);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
