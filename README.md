hw-hadoop-pagerank
===========================
##Compile##

~~~console
$ cd pagerank
$ export JAVA_HOME=/usr/lib/jvm/java-7-oracle/
$ ./gogo.sh -c
~~~  

##Execute##
Build the graph of titles and links

~~~console
$ ./gogo.sh -i <INPUT_PATH> -o <OUTPUT_PATH> -g
~~~
Count the PageRank

~~~console
$ ./gogo.sh -i -i <INPUT_PATH> -o <OUTPUT_PATH> [-n <iterative_times> -p
~~~

## Detail ##
### Step 1:Build a graph(2 MapReduce)###
1 MapReduce for removing no-title-link and dangling node
#### 1 MapReduce ####
#####Mapper：#####
Split and take <Key , Value> 
Form `<title>` `title` `</title>` `[[` `node` `]]` 
to

- `<node##Title , []>`    [] means root which points to every title
- `<[]##Title , node>` 
- `<node##link , title>`   Link points to Title

#####Partitioner：#####

Make sure the same node in Key `[node##Title]` or `[node##link]` will be organized to the same reducer.

#####GroupComparator：#####
Group the Key `[node##Title]` `[node##link]` together
and the root node `[]` will be organized to a single reducer
#####Reducer：#####
Get Output<Key, Value>
    `<[node##Title] , [ ]>`, `< [node##link] , [title]>` 
If get the Key `[node##link]` rather than `[node##Title]` return nothing
Else

Key: `Text ([Title]) `
Value: `Text([node])`

For dangling node to count the links of the node, if there is only `[]` link, this node is the dangling node.
#### 2 MapReduce ####
Group the several values of node to the one value with `|`

### Step 2: Count PageRank ###
2 mapreduce for one iterative

#### 1 MapReduce ####
Give the score of this node to other node which it points to.

#### 2 MapReduce ####

The dangling nodes give their scores to root node `[]` so during the second mapreduce root node give the scores from dangling nodes to all nodes.


