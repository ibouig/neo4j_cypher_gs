# neo4j_cypher_gs

Work in progress!

## DONE

1) ~~Init env, Neo4j,Twitter4j~~
2) ~~Create Twitter graph model with Cypher~~
3) ~~Stream tweets with maximum information~~
5) ~~Query dataset from Neo4j database~~
6) Create a web-based player to visualize time-based graphs with Neo4j
7) exporting specific time-based data
8) Tensorflow models
9)

![Imgur](https://github.com/ibouig/neo4j_cypher_gs/blob/master/screen.png)



MATCH (n)-[r]->(m) RETURN n,r,m;

echo "match(n) return n;" > query.txt
cat query.txt | ./cypher-shell --format plain >> out.txt
cat query.txt | ./cypher-shell -u neo4j -p neo4j --format plain >> out.txt

neo4j-shell -file query.cql > out.txt
