#if [ $1 == '-c' ] ; then
#../apache-maven-3.3.3/bin/mvn clean compile assembly:single
#fi 
#
#hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.PageRank /opt/Assignment3/Input/100M/input-100M output 


while getopts "i:o:n:cgpht" opt; do
    case "$opt" in
    h|\?)
	echo $' -c\t:compile \n -i [INPUT_PATH]\t:input\n -o [OUPUT_PATH]\t:output\n -g\t: build a graph\n -p\t: pagerank'
        exit 0
        ;;
    i) 
	input=$OPTARG
	;;
    o)
	output=$OPTARG
	;;
    c) 
	mode="compile"
        ;;
    g)
	mode="graph"
	;;
    p)
	mode="pagerank"
	;;
    n)
	timesOption="-times"
	timesNumber=$OPTARG
	;;
    esac
done

if [ "$mode" == "compile" ] 
then
    ../apache-maven-3.3.3/bin/mvn clean compile assembly:single
elif [ "$mode" == "graph" ]	 
then
   hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.PageRank -Dmapreduce.job.maps=200 "$input" "$output" -graph 
elif [ "$mode" == "pagerank" ]
then
   hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.PageRank "$input" "$output" "$timesOption" "$timesNumber" -pagerank
#elif [ "$mode" == "loadbase" ]
#then
#   hadoop jar target/pagerank-103062505-jar-with-dependencies.jar hw3.LoadHBase "$input" "$output" 
fi

