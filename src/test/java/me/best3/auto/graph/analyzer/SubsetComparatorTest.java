package me.best3.auto.graph.analyzer;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import me.best3.auto.graph.index.Document;
import me.best3.auto.graph.index.FileIndexerTest;
import me.best3.auto.graph.index.LocalFileSystemIndexer;

public class SubsetComparatorTest {
	private static final String DOCINDEX = "./docindex";
	private static final String SUBSET_COMPARATOR_TEST_JSON = "SubsetComparatorTest.json";
	private static LocalFileSystemIndexer localFSIndexer;
	private static String testJsonFile = FileIndexerTest.class.getClassLoader().getResource(SUBSET_COMPARATOR_TEST_JSON).getFile();
	
	@BeforeAll
	public static void setup() {
		try {
			localFSIndexer = new LocalFileSystemIndexer(DOCINDEX);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterAll
	public static void tearDown() {
		if(SubsetComparatorTest.localFSIndexer!=null) {
			try {
				SubsetComparatorTest.localFSIndexer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void processTokens() throws IOException, InterruptedException {
		localFSIndexer.processJSONFile(testJsonFile);
		SubsetComparator subsetComparator = new SubsetComparator();
		List<Document> documents = localFSIndexer.getDocuments(subsetComparator);
		documents.forEach(d -> {
			try {
				System.out.println(d.toJSON());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

}
