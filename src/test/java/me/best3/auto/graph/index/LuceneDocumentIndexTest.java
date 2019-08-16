package me.best3.auto.graph.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LuceneDocumentIndexTest {
	private static final String FILE_INDEXER_TEST_INDEX = "./fileIndexerTestIndex/docindex";
	private static LuceneDocumentIndex docIndex;
	
	private Document match12 = new Document();
	
	public LuceneDocumentIndexTest() {
		match12.addString("fieldOne", "fieldOne");
		match12.addString("fieldTwo", "fieldTwo");
	}

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
		if (LuceneDocumentIndexTest.docIndex != null) {
			try {
				LuceneDocumentIndexTest.docIndex.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testFindAll() {
		
		try {
			List<Document> results = LuceneDocumentIndexTest.docIndex.find(match12);
			assertEquals(1, results.size(),"Expected only one doc in search results.");
			assertNotNull(results.get(0).get("fieldOne"), "FieldOne is expected on the document returned.");
			assertNotNull(results.get(0).get("fieldTwo"), "FieldTwo is expected on the document returned.");
//			results.stream().map(d -> {
//				try {
//					return d.toJSON();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				return "";
//			}).forEach(System.out::println);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void countTest() {
		try {
			int count = LuceneDocumentIndexTest.docIndex.count(match12);
			assertEquals(1, count,"Expected one document");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void exactMatchTest() {
		try {
			List<Document> results = LuceneDocumentIndexTest.docIndex.exactMatches(match12);
			assertEquals(0, results.size(),"Expected no results as these documents have ID field in there.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
