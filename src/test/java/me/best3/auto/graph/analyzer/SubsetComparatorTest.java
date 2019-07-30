package me.best3.auto.graph.analyzer;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import me.best3.auto.graph.index.Document;
import me.best3.auto.graph.index.FileIndexer;
import me.best3.auto.graph.index.FileIndexerTest;

public class SubsetComparatorTest {
	private static final String SUBSET_COMPARATOR_TEST_JSON = "SubsetComparatorTest.json";
	private static FileIndexer fileIndexer;
	private static String testJsonFile = FileIndexerTest.class.getClassLoader().getResource(SUBSET_COMPARATOR_TEST_JSON).getFile();
	
	@BeforeAll
	public static void setup() {
		try {
			fileIndexer = new FileIndexer();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterAll
	public static void tearDown() {
		if(SubsetComparatorTest.fileIndexer!=null) {
			try {
				SubsetComparatorTest.fileIndexer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void processTokens() throws IOException, InterruptedException {
		fileIndexer.processJSONFile(testJsonFile);
		SubsetComparator subsetComparator = new SubsetComparator();
		List<Document> documents = fileIndexer.getDocuments(subsetComparator);
		documents.forEach(d -> {
			try {
				System.out.println(d.toJSON());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

}
