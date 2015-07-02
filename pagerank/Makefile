TARGET=pagerank-103062505-jar-with-dependencies.jar

allp: clean $(TARGET) rung
runp:
	hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.PageRank /opt/Assignment3/Input/100M/input-100M output
rung:
	hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.Graph /opt/Assignment3/Input/100M/input-100M output

$(TARGET): $(wildcard src/main/java/hw3/*.java)
	../apache-maven-3.3.3/bin/mvn compile assembly:single
clean:
	../apache-maven-3.3.3/bin/mvn clean




