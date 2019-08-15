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
	private static final String DOCINDEX = "./subsetcomp";
	private static final String SUBSET_COMPARATOR_TEST_JSON = "SubsetComparatorTest.json";
	private static LocalFileSystemIndexer localFSIndexer;
	protected static String TEST_JSON_FILE = FileIndexerTest.class.getClassLoader().getResource(SUBSET_COMPARATOR_TEST_JSON).getFile();
	
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
		localFSIndexer.processJSONFile(TEST_JSON_FILE);
		SubsetComparator subsetComparator = new SubsetComparator();
		List<Document> documents = localFSIndexer.getAllDocuments(subsetComparator);
		documents.forEach(d -> {
			try {
				System.out.println(d.toJSON());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

}
