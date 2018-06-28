package nl.netage.stardog.describe;

import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import com.complexible.common.base.CloseableIterator;
import com.complexible.common.base.Streams;
import com.complexible.common.openrdf.query.ImmutableDataset;
import com.complexible.common.rdf.model.Namespaces;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.plan.describe.DescribeStrategy;
import com.complexible.stardog.query.Query;
import com.complexible.stardog.query.QueryFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.openrdf.model.BNode;
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

	public final static String NAME = "NetageDescribeStrategy";

	private final static String SUBJECT = "subject";
	private final static String PREDICATE = "predicate";
	private final static String OBJECT = "object";

	@Override
	public String getName() {
		return NAME;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Stream<Statement> describe(final QueryFactory theQueryFactory, final Dataset theDataset, final Resource theValue) throws StardogException {
		Set<Resource> aVisited = Sets.newHashSet();
		Queue<Resource> aTodo = Queues.newArrayDeque();
		Dataset aDataset = ImmutableDataset.builder().namedGraphs(Iterables.concat(theDataset.getDefaultGraphs(), theDataset.getNamedGraphs())).build();
		String aStatementPattern = "graph ?g { ?" + SUBJECT + " ?" + PREDICATE + " ?" + OBJECT + " }";
		String aHashPrefix = theValue.stringValue() + "#";
		Query<GraphQueryResult> aCBDQuery = theQueryFactory.graph("construct { " + aStatementPattern + " } where { " + aStatementPattern + " }", Namespaces.STARDOG)
		                                                   .dataset(aDataset);

		aTodo.add(theValue);

		return Streams.stream(new CloseableIterator.AbstractCloseableIterator<Statement>() {
			CloseableIterator<Statement> mStatements = CloseableIterator.empty();

			@Override
			public void close() {
				mStatements.close();
			}

			@Override
			protected Statement computeNext() {
				for (;;) {
					if (mStatements.hasNext()) {
						Statement aNext = mStatements.next();

						addToQueue(aNext, aTodo, aHashPrefix, aVisited);

						return aNext;
					}

					Resource aNext = aTodo.poll();

					if (aNext == null) {
						return endOfData();
					}

					mStatements.close();
					mStatements = runQuery(aCBDQuery, aNext);
				}
			}
		});
	}

	private CloseableIterator<Statement> runQuery(final Query<GraphQueryResult> theCBDQuery, final Resource theResource) {
		GraphQueryResult aResults = theCBDQuery.parameter(SUBJECT, theResource).execute();

		return new CloseableIterator.AbstractCloseableIterator<Statement>() {
			@Override
			public void close() {
				aResults.close();
			}

			@Override
			protected Statement computeNext() {
				if (aResults.hasNext()) {
					return aResults.next();
				}

				return endOfData();
			}
		};
	}

	private void addToQueue(final Statement theStatement, final Queue<Resource> theQueue, final String theHashPrefix, final Set<Resource> theVisited) {
		if ((theStatement.getObject() instanceof BNode || theStatement.getObject().stringValue().startsWith(theHashPrefix)) &&
		    theVisited.add((Resource) theStatement.getObject())) {
			theQueue.add((Resource) theStatement.getObject());
		}
	}
}