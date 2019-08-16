package me.best3.auto.graph.schema;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import me.best3.auto.graph.analyzer.SubsetComparator;
import me.best3.auto.graph.index.Document;
import me.best3.auto.graph.index.DocumentWithID;
import me.best3.auto.graph.index.VisitableDocument;

/**
 * Processes each document and sets up parent child relationships based on
 * algorithm
 * 
 * placeDoc ( doc , graph ): for each node in graph
 * 
 * @author spottur2
 *
 */
public class BuildGraph implements Consumer<Document> {
	private static final Logger logger = LogManager.getLogger(BuildGraph.class);
	private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();
	private DirectedAcyclicGraph<Document, DefaultEdge> graph;
	// need a set of class roots to make navigation easy

	public BuildGraph(DirectedAcyclicGraph<Document, DefaultEdge> graph) {
		this.graph = graph;
	}

	/**
	 * find all supersets of this document and add edges from subsets to superset
	 */
	@Override
	public void accept(Document doc) {
		if (DEBUG_ENABLED) {
			logger.debug(String.format("before : graph edge set size  : %d", graph.edgeSet().size()));
		}
		List<VisitableDocument<DocumentWithID>> supersets = findAllSupersetsOf(doc);
		if (DEBUG_ENABLED) {
			logger.debug(String.format("subsets filter size  : %d", supersets.size()));
		}
		processSupersets(doc, supersets);
		if (DEBUG_ENABLED) {
			logger.debug(String.format("after : graph edge set size  : %d", graph.edgeSet().size()));
		}
	}

	private void processSupersets(Document doc, List<VisitableDocument<DocumentWithID>> supersets) {
		supersets.stream().filter(d -> {
			return !new DocumentWithID(d.getWrappedDocument()).equals(new DocumentWithID(doc));
		})// filter out itself to avoid self loops
				.peek(d -> d.setVisisted(true)).forEach(d -> addEdge(d, doc));
	}

	private List<VisitableDocument<DocumentWithID>> findAllSupersetsOf(Document doc) {
		return graph.vertexSet().stream().map(d -> {
			return new VisitableDocument<DocumentWithID>(new DocumentWithID(d));
		}).filter(d -> SubsetComparator.isSubSet(doc, d.getWrappedDocument())).filter(VisitableDocument::isNotVisited)
				.collect(Collectors.toList());
	}

	private void addEdge(VisitableDocument<DocumentWithID> superset, Document subset) {
		graph.addEdge(subset, superset.getWrappedDocument());
	}

}
