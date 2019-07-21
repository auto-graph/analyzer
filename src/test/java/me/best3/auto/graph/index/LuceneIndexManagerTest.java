package me.best3.auto.graph.index;

import java.io.IOException;

import junit.framework.TestCase;
import me.best3.auto.graph.index.LuceneIndexManager.LuceneIndex;

public class LuceneIndexManagerTest extends TestCase {
	
	private LuceneIndex index;

	protected void setUp() throws Exception {
		super.setUp();
		this.index = new LuceneIndexManager().getLuceneIndexManager();
	}

	public void testGetLuceneIndexManager() {
		try {
			assertEquals("Singleton working . ", new LuceneIndexManager().getLuceneIndexManager().hashCode(), this.index.hashCode());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testWrite() {
		try {
			this.index.write("Test", "Test value");
		} catch (IOException e) {
			e.printStackTrace();
		}
		read();
	}

	public void read() {
		try {
			assertEquals("Read operation working", "Test value",this.index.read("Test"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
