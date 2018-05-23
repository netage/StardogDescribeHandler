# StardogDescribeHandler

Custom Describe handler for stardog. Handles resources(http://example.com/1) and hashresources(http://example.com/1#content) as one resource.

## installation

To install the custom describe handler:

(change repository for the repository name, you want to use the describe handler with)

- Package with maven.
- copy the jar to the sever/dbms map.
- Start the stardog 5.3.0 server.
- stardog-admin.bat metadata set repository -o query.describe.strategy=NetageDescribeStrategy
- Use stardog-admin.bat metadata get repository
- check whether the query.describe.strategy option is set to NetageDescribeStrategy

By using the DESCRIBE <uri> the custom describe handler is used. 

## example

```
<http://www.example.com/subject1> a vocab:Subject;
                rdfs:label "Subject 1";
                dc:identifier "subject1" ;
                vocab:content <http://www.example.com/subject1#content> .

<http://www.example.com/subject1#content> a vocab:Content;
               rdf:value "Content of the subject".
```

With the default DESCRIBE on the <http://www.example.com/subject1>, we only would find the following block of triples:
```
<http://www.example.com/subject1> a vocab:Subject;
                rdfs:label "Subject 1";
                dc:identifier "subject1" ;
                vocab:content <http://www.example.com/subject1#content> .
```

With the custom Describe handler we will find also the related hashed subjects:
```
<http://www.example.com/subject1> a vocab:Subject;
                rdfs:label "Subject 1";
                dc:identifier "subject1" ;
                vocab:content <http://www.example.com/subject1#content> .

<http://www.example.com/subject1#content> a vocab:Content;
               rdf:value "Content of the subject".
```