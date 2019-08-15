package me.best3.auto.graph.schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.junit.jupiter.api.Test;

import me.best3.auto.graph.analyzer.SubsetComparatorTest;
import me.best3.auto.graph.index.Document;
import me.best3.auto.graph.index.DocumentWithID;

public class BuilderTest extends SubsetComparatorTest{
	
	
	@Test
	public void schemaBuilderTest() {
		
		InheritenceDAGBuilder builder = new InheritenceDAGBuilder();
		ComponentNameProvider<Document> vertexIDProvider = (d) -> {
			return String.valueOf(d.get(DocumentWithID.ID_FIELD).hashCode());
		};
		
		ComponentNameProvider<Document> vertexLabelProvider = (d) ->{
			return d.getFields().stream().map(f -> {
				return f.replace("field", "");
			}).collect(Collectors.joining());
		};

		DirectedAcyclicGraph<Document, DefaultEdge> graph = builder.deduceSchema(SubsetComparatorTest.TEST_JSON_FILE);
		 
		 try {
			new DOTExporter<Document, DefaultEdge>(vertexIDProvider, vertexLabelProvider, null).exportGraph(graph,
						Files.newOutputStream(Paths.get("./", "graph.gv"), StandardOpenOption.CREATE));
		} catch (ExportException | IOException e) {
			e.printStackTrace();
		}
		
	}

}