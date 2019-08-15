package me.best3.auto.graph.index;

public class VisitableDocument <T extends Document> {
		private boolean visisted;
		private T wrappedDocument;
		
		public VisitableDocument(T doc) {
			this.wrappedDocument = doc;
		}
		
		public void setVisisted(boolean markVisited) {
			this.visisted = markVisited;
		}
		
		public boolean isVisited() {
			return this.visisted;
		}
		
		public boolean isNotVisited() {
			return !isVisited();
		}

		public T getWrappedDocument() {
			return wrappedDocument;
		}

		public void setWrappedDocument(T wrappedDocument) {
			this.wrappedDocument = wrappedDocument;
		}
		
}
