package me.best3.auto.graph.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class LuceneIndexManagerTest {
	
	private LuceneKVIndex index;
	
	@BeforeAll
	public static void setUp() throws Exception {
		new LuceneIndexFactory().getLuceneKVIndex();
//		ResultPrinter resultPrinter = new re
	}
	
	@BeforeEach
	public void setupTest() throws IOException {
		this.index = new LuceneIndexFactory().getLuceneKVIndex();
	}

	@Test
	@RepeatedTest(3)
	public void testGetLuceneIndexManager() {
		try {
			assertEquals( new LuceneIndexFactory().getLuceneKVIndex().hashCode(), this.index.hashCode(),"Singleton working . ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	@RepeatedTest(3)
	public void testWrite() {
		try {
			this.index.write("Test", "Test value " + new Date());
			
			//Pay the price for refreshing the reader since this is a test
			this.index.refreshReader();
			
			//read it back
			String value = this.index.read("Test");
			assertNotNull(value,"Read value is null ");
			if(value!=null) {
				assertTrue(value.startsWith("Test value"),"Read operation working");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	@RepeatedTest(3)
	public void testUpdate() {
		try {
			//pay the price for refreshing the reader, its OK as this is just a test 
			this.index.refreshReader();
			long docCount = this.index.getDocCount();
			//the very first time we know we expect 2 documents,
			//but index is not aware of the incoming doc write that update does
			if(this.index.read("WriteTest") == null) {
				docCount++;
			}
			this.index.update("WriteTest", "WriteTest-updated");
			//pay the price for refreshing the reader, its OK as this is just a test 
			this.index.refreshReader();
			long newDocCount =  this.index.getDocCount();
			//since update deletes and adds new doc, and doc count 
			//returns all documents including deleted documents
			assertEquals(docCount, newDocCount,"Document count different");
			
			
			assertEquals(this.index.read("WriteTest"),"WriteTest-updated","Updated value matched");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	@AfterAll
	public  static void tearDown() throws Exception {
		new LuceneIndexFactory().getLuceneKVIndex().debugDumpIndex();
	}
	
	

}
