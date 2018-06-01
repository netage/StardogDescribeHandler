package nl.netage.stardog.describe;

import java.util.ArrayList;
import java.util.stream.Stream;

import com.complexible.common.base.CloseableIterator;
import com.complexible.common.base.Streams;
import com.complexible.common.openrdf.query.ImmutableDataset;
import com.complexible.common.rdf.model.Namespaces;
import com.complexible.stardog.plan.describe.DescribeStrategy;
import com.complexible.stardog.query.QueryFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;

/**
 * Example {@link DescribeStrategy} which returns all statements where the node occurs as a subject or as an object.
 *
 * @author  Netage
 */
public final class NetageDescribeStrategy implements DescribeStrategy {
	static String BASE_SUBJECT = "";

	@SuppressWarnings("unchecked")
	public Stream<Statement> describe(final QueryFactory theFactory, final Dataset theDataset, final Resource theValue) {
		// This class can be simplified by extending SingleQueryDescribeStrategy but we're showing the full version
		// in case you want to do more than just run a single query.
		Preconditions.checkArgument(theValue != null, "The described value should not be null");
		BASE_SUBJECT = theValue.stringValue();
		// The SPARQL spec doesn't define the dataset for DESCRIBE queries.
		// FROM [NAMED] clauses in DESCRIBE queries define it for the WHERE pattern but
		// it doesn't say whether the same dataset should be used for describing matched resources.
		// It should be otherwise queries like DESCRIBE :A FROM :g don't make sense but one may want to
		// describe resources matched in G1 based on information in G2.
		
		return Streams.stream(retrieveResults(theFactory, theDataset, theValue.stringValue()));
		//return Streams.concat(Streams.stream(aResultsIter), Streams.stream(bResultsIter), Streams.stream(cResultsIter));
	}
	
	private CloseableIterator<Statement> retrieveResults(final QueryFactory theFactory, final Dataset theDataset, final String theValue){
		final CloseableIterator<Statement> streams;
		
		Dataset aDataset = ImmutableDataset.builder()
				.namedGraphs(Iterables.concat(theDataset.getDefaultGraphs(), theDataset.getNamedGraphs()))
				.build();
		
		final GraphQueryResult aResults = theFactory.graph("CONSTRUCT {?s ?p ?o} WHERE { Graph ?g {"
				+ "?s ?p ?o}}",                                                                                
				Namespaces.STARDOG)
				.dataset(aDataset)
				.parameter("s", theValue)
				.execute();
		
		CloseableIterator<Statement> resultsIter = new CloseableIterator.AbstractCloseableIterator<Statement>() {
			public void close() {
				aResults.close();
			}

			@Override
			protected Statement computeNext() {
				if (aResults.hasNext()) {
					System.out.println(aResults.next().getObject().stringValue() + " - " + BASE_SUBJECT+"#");
					if(aResults.next().getObject().stringValue().startsWith(BASE_SUBJECT+"#")){
						streams = Streams.concat(streams,retrieveResults(theFactory, theDataset, aResults.next().getObject().stringValue()));
					}

					return aResults.next();
				}

				return endOfData();
			}
		};
		
		return Streams.stream(resultsIter);
	}


	public String getName() {
		return "NetageDescribeStrategy";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Describe(NetageDescribeStrategy)";
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		return obj instanceof NetageDescribeStrategy;
	}
}
