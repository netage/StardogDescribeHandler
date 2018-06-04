package nl.netage.stardog.describe;

import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

import com.complexible.common.base.Streams;
import com.complexible.common.openrdf.query.ImmutableDataset;
import com.complexible.common.rdf.model.Namespaces;
import com.complexible.stardog.plan.describe.DescribeStrategy;
import com.complexible.stardog.query.QueryFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;

/**
 * Example {@link DescribeStrategy} which returns all statements where the node occurs as a subject or as an object.
 *
 * @author  Netage
 */
public final class NetageDescribeStrategy implements DescribeStrategy {
	static String BASE_SUBJECT = "";
	ValueFactory vfac = SimpleValueFactory.getInstance();
	
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
		
		// build hashset
		HashSet<Statement> collection = traverseResults(theFactory, theDataset, theValue);
		System.out.println("Size: "+collection.size());
		final Iterator<Statement> itr = collection.iterator();
		
		return Streams.stream(itr);
		
	}
	
	HashSet<Statement> traverseResults(final QueryFactory theFactory, final Dataset theDataset, final Resource theValue){
		HashSet<Statement> statements = new HashSet<Statement>();
		
		Dataset aDataset = ImmutableDataset.builder()
				.namedGraphs(Iterables.concat(theDataset.getDefaultGraphs(), theDataset.getNamedGraphs()))
				.build();
		
		final GraphQueryResult aResults = theFactory.graph("CONSTRUCT {?s ?p ?o} WHERE { Graph ?g {"
				+ "?s ?p ?o}}",                                                                                
				Namespaces.STARDOG)
				.dataset(aDataset)
				.parameter("s", theValue)
				.execute();
		
		while(aResults.hasNext())
		{
			Statement statement = aResults.next();
			System.out.println("While next! ( " + statement.getSubject().stringValue() + " - " + statement.getPredicate().stringValue() + " - " + statement.getObject().stringValue() +")");
			if(statement.getObject().stringValue().startsWith(BASE_SUBJECT+"#")){	
				statements.addAll(traverseResults(theFactory, theDataset, vfac.createIRI(statement.getObject().stringValue())));
			}else{
				statements.addAll(traverseResults(theFactory, theDataset, vfac.createBNode(statement.getObject().stringValue())));
			}
			statements.add(statement);
		}		
		//System.out.println("Size local: "+statements.size());
		return statements;
	
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
