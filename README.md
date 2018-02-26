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





#How to run
go to directory after clone

    mvn clean install assembly:single

After compile run :

    java -jar target/neo4jTwitter-1.0-SNAPSHOT-jar-with-dependencies.jar
    
    
# To export data from database

from browser 

    MATCH (n)-[r]->(m) RETURN n,r,m;
    
    call apoc.export.csv.query("MATCH (n)-[r]->(m) RETURN n,r,m","results.csv",{})

from console 

    echo "match(n) return n;" > query.txt
    cat query.txt | ./cypher-shell --format plain >> out.txt
    cat query.txt | ./cypher-shell -u neo4j -p neo4j --format plain >> out.txt

    neo4j-shell -file query.cql > out.txt    
    
Or Run script :

    sudo chmod u+x ExportScript.sh && sudo ./ExportScript.sh
