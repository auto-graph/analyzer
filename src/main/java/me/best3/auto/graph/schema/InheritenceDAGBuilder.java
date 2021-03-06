package me.best3.auto.graph.schema;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import me.best3.auto.graph.analyzer.SubsetComparator;
import me.best3.auto.graph.index.Document;
import me.best3.auto.graph.index.LocalFileSystemIndexer;

public class InheritenceDAGBuilder {
	private static final Logger logger = LogManager.getLogger(InheritenceDAGBuilder.class);
	private LocalFileSystemIndexer localFSIndexer;
	
	private DirectedAcyclicGraph<Document, DefaultEdge> graph = new DirectedAcyclicGraph<Document, DefaultEdge>(DefaultEdge.class);
	
	public InheritenceDAGBuilder(LocalFileSystemIndexer localFSIndexer) {
		this.localFSIndexer = localFSIndexer;
	}

	/**
	 * Input Vertex set roughly ordered by subsets ahead of supersets
	 * #1 Build a DAG includes all documents with edges from subset to superset
	 * #2 Perform a topological sort 
	 * 			Note : this will improve performance of subsequent scans
	 * #3 For each vertex in the vertex list in the graph
	 * 	#3a Get all paths between this element to every other element in the array
	 *   
	 * @param dataFile
	 * @return
	 */
	public void deduceSchema(String dataFile) {
		try {
			
			
			// TODO: need mechanism to fragment data file, for scaling this
			localFSIndexer.processJSONFile(dataFile);
			
			//build the graph
			buildSubsetToSupersetGraph();
			
		} catch (IOException e) {
			logger.error(e, e);
		}
	}

	/**
	 * #3b Mark all edges that are processed
	 *  		Note : Optimization
	 *  #3c Remove edges that are connecting a non minimal subset to a superset
	 *  #3d Once we processes all edges, stop processing further vertices as we are done
	 */
	public void removeNonMinimalSubsetEdges() {
		long topSortStartTime = 0;
		long topSortEndTime = 0;
		if(logger.isDebugEnabled()) {
			topSortStartTime = System.currentTimeMillis();
		}
		
		//perform a topological sort of this DAG
		Document[] sortedVertices = performTopologicalSort(); 
		
		if(logger.isDebugEnabled()) {
			topSortEndTime = System.currentTimeMillis();
			logger.debug(String.format("Time to do topological sort %d", (topSortEndTime-topSortStartTime)));
		}

		AllDirectedPaths<Document, DefaultEdge> directPaths = new AllDirectedPaths<>(graph);
		
		//clone edge set for marking of processed edges
		Set<DefaultEdge> edgeSet = new HashSet<>();
		edgeSet.addAll(graph.edgeSet());// make a copy so we can modify safely
		
		long minimizeStartTime = 0;
		long minimizeEndTime = 0;
		if(logger.isDebugEnabled()) {
			minimizeStartTime = System.currentTimeMillis();
		}
		
		int i = 0;
		while (i < sortedVertices.length) {
			Document baseVertex = sortedVertices[i];
			int k = i + 1;
			while (k < sortedVertices.length && edgeSet.size() > 0) {
				List<GraphPath<Document, DefaultEdge>> paths = getSortedListOfPaths(directPaths,baseVertex, sortedVertices[k]);

				// optimization : when all edges are covered we can stop
				markEdges(edgeSet, paths);
				
				removeImplicitSubsetEdges(paths);

				k++;
			}
			i++;
		}
		
		if(logger.isDebugEnabled()) {
			minimizeEndTime = System.currentTimeMillis();
			logger.debug(String.format("Time to remove non minimal edges %d", (minimizeEndTime-minimizeStartTime)));
		}
	}

	private void removeImplicitSubsetEdges(
			List<GraphPath<Document, DefaultEdge>> paths) {
		if(paths.size()>1) {
			GraphPath<Document, DefaultEdge> longestPath = paths.get(paths.size()-1);
			paths.stream().limit(paths.size()-1).forEach(p->{// remove all but longest paths
				p.getEdgeList().stream().forEach(e->{//only remove edges that are not part of longest path
					if(!longestPath.getEdgeList().contains(e)) {
						this.graph.removeEdge(e);
					}
				});
			});
		}
	}

	private void markEdges(Set<DefaultEdge> edgeSet, List<GraphPath<Document, DefaultEdge>> paths) {
		paths.forEach(p -> {
			edgeSet.removeAll(p.getEdgeList());
		});
	}

	private List<GraphPath<Document, DefaultEdge>> getSortedListOfPaths(AllDirectedPaths<Document, DefaultEdge> directPaths,
			Document sourceVertex,
			Document targetVertex) {
		List<GraphPath<Document, DefaultEdge>> paths = directPaths.getAllPaths(sourceVertex,
				targetVertex, false, 10000);
		Collections.sort(paths, new Comparator<GraphPath<Document, DefaultEdge>>() {

			@Override
			public int compare(GraphPath<Document, DefaultEdge> p1, GraphPath<Document, DefaultEdge> p2) {
				return Integer.compare(p1.getLength(), p2.getLength());
			}
		});
		return paths;
	}

	private Document[] performTopologicalSort() {
		TopologicalOrderIterator<Document, DefaultEdge> toi = new TopologicalOrderIterator<>(this.graph);
		Document[] sortedVertices = new Document[this.graph.vertexSet().size()];
		int j = 0;
		while (toi.hasNext()) {
			sortedVertices[j] = toi.next();
			j++;
		}
		return sortedVertices;
	}

	private void buildSubsetToSupersetGraph() throws IOException {
		long buildGraphStartTime = 0;
		long buildGraphEndTime = 0;
		if(logger.isDebugEnabled()) {
			buildGraphStartTime = System.currentTimeMillis();
		}
		
		BuildGraph docConsumer = new BuildGraph(this.graph);
		SubsetComparator subsetComparator = new SubsetComparator();
		List<Document> allDocs = localFSIndexer.getAllDocuments(subsetComparator);
		allDocs.stream().forEach(this.graph::addVertex); // Add vertices
		allDocs.stream().forEach(docConsumer); // Add links from subset to super set
		
		if(logger.isDebugEnabled()) {
			buildGraphEndTime = System.currentTimeMillis();
			logger.debug(String.format("Time to build graph %d", (buildGraphEndTime-buildGraphStartTime)));
		}
	}

	public DirectedAcyclicGraph<Document, DefaultEdge> getGraph() {
		return graph;
	}

}
