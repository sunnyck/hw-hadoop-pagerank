package hw3;

import java.io.IOException;
import java.util.*;
import java.util.regex.MatchResult;
import java.util.ArrayList;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.commons.collections.IteratorUtils;

import org.apache.commons.cli.*;

import com.google.common.collect.Iterables;


public class PageRank  extends Configured implements Tool {
	private final Logger log = Logger.getLogger(getClass());


	public static class BuildMap extends Mapper<LongWritable, Text, Text, Text>{   

		public void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Scanner sc = new Scanner(line).useDelimiter("\n");
			Text title = new Text();

			while (sc.hasNext()) {
				String linktmp[];
				String titletmp[];
				String links = "";
				String linkstmp[];
				titletmp = sc.next().split("(<\\/?title>)");
				title.set(String.format("%s", titletmp[1]));
				context.write(new Text(titletmp[1]+"##Title"), new Text("[]"));
				linkstmp = titletmp[2].split("\\[\\[");
				context.write(new Text("[]##Title"), new Text(titletmp[1]));
				for(String link: Arrays.copyOfRange(linkstmp, 1, linkstmp.length)){
					linktmp = link.split("\\]\\]");
					context.write(new Text(linktmp[0] + "##link"), title);
				}
			}


		}
	}
	public static class BuildPartitioner extends Partitioner<Text, Text>{
		@Override
			public int getPartition(Text from, Text to , int numPartitions){
				String titlelink[] = from.toString().split("##");
				if(titlelink[0].equals("[]"))
					return (numPartitions -1);
				else		
					return (titlelink[0].hashCode() & Integer.MAX_VALUE ) % (numPartitions -1);
			}
	}

	public static class BuildGroupComparator extends WritableComparator {
		protected BuildGroupComparator() {
			super(Text.class, true);
		}
		@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				String termw1[] = ((Text)w1).toString().split("##");
				String termw2[] = ((Text)w2).toString().split("##");
				return (termw1[0].compareTo(termw2[0]));
			}
	}
	public static class BuildReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce (Text node, Iterable<Text> titles, Context context)
			throws IOException, InterruptedException {
				String nodeitem[] = node.toString().split("##");
				if(nodeitem[1].equals("Title")){
					for(Text title:titles){
						context.write(title, new Text(nodeitem[0]));	
					}

				}
				else{
				//	for(Text title:titles){
				//		context.write(title, new Text("[]"));
				//	}
				}
			}
	}
	public static class GraphMap extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value); 
		}
	}
	
	public static class GraphPartitioner extends Partitioner<Text, Text>{
                @Override
                        public int getPartition(Text from, Text to , int numPartitions){
                                String title = from.toString();
				//String titlelink[] = from.toString().split("##");
                                if(title.equals("[]"))
                                        return (numPartitions -1);
                                else
                                        return (title.hashCode() & Integer.MAX_VALUE ) % (numPartitions -1);
                        }
        }


	public static class GraphCombine extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text title, Iterable<Text> links, Context context) throws IOException, InterruptedException {
				/*Iterator<Text> links_it = links.iterator();
				String values = links_it.next().toString();
                                while(links_it.hasNext() && title.toString().equals("[]")){					
					Text link = links_it.next();
					if(!link.toString().equals(""))
                                        	values = values + "|" +link;
				}			
				context.write(title, new Text(values));
				*/
				String values = "";
				for(Text link:links){
					if(title.toString().equals("[]")){
						//if(!link.toString().equals(""))
                                                values = values + "|" +link;
					}
					else
						context.write(title, link);
				}
				if(title.toString().equals("[]"))
					context.write(title, new Text(values));
				
			}
	}


	public static class GraphReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce (Text title, Iterable<Text> links, Context context)
			throws IOException, InterruptedException {
				int counter = 0;
				String values = "";
				for(Text link:links){
					counter ++;
					if(!link.toString().equals("[]"))
						values = values +"|"+ link ;
				}
				if(counter == 1)
					values = values +"|[]";
				if(title.toString().equals("[]"))
					context.write(title, new Text("0.0]]"+values));
				else
					context.write(title, new Text("1.0]]"+values));
				
			}
	}
       public static class CountMap extends Mapper<Text, Text, Text, Text>{

                public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String scoreitem[] = value.toString().split("\\]\\]");
			String links[] = scoreitem[1].split("\\|"); 
			
			if(!key.toString().equals("[]")){
				Double score = Double.valueOf(scoreitem[0]);
				for(int i = 1; i<links.length; i++){
					if(links[i].equals("[]"))
						context.write(new Text("[]"), new Text(String.valueOf(score*1.0/(links.length-1))+"]]"));
					else
						context.write(new Text(links[i]), new Text(String.valueOf(score*0.85/(links.length-1))+"]]"));
				}
			}

			context.write(key, new Text("0.0]]"+scoreitem[1]));

                }
        }
	public static class Count2Map extends Mapper<Text, Text, Text, Text>{

                public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                        String scoreitem[] = value.toString().split("\\]\\]");

                        String links[] = scoreitem[1].split("\\|");
                        Double score = Double.valueOf(scoreitem[0]);
                        if(key.toString().equals("[]")){
				for(int i = 1; i<links.length; i++)
                                        context.write(new Text(links[i]), new Text(String.valueOf(score*0.85/(links.length-1))+"]]"));
			
                        	context.write(key, new Text("0.0]]"+scoreitem[1]));
			}
			else
				context.write(key, value);

                }
        }
	public static class CountPartitioner extends Partitioner<Text, Text>{
                @Override
                        public int getPartition(Text from, Text to , int numPartitions){
                                if(from.toString().equals("[]"))
                                        return (numPartitions -1);
                                else
                                        return (from.toString().hashCode() & Integer.MAX_VALUE ) % (numPartitions -1);
                        }
        }
	public static class CountCombine extends Reducer<Text, Text, Text, Text>{
                public void reduce (Text node, Iterable<Text> scoresFromOther, Context context)
                        throws IOException, InterruptedException {
                                Double scoreSum = 0.0;
                                String links = "";
                                for(Text scoreFromOther:scoresFromOther){
                                        String scoreitem [] = scoreFromOther.toString().split("\\]\\]");
                                        scoreSum = scoreSum + Double.valueOf(scoreitem[0]);
                                        for(int i = 1; i< scoreitem.length; i++)
                                                links = links + "" + scoreitem[i];
                                }
                                context.write(node, new Text(String.valueOf(scoreSum)+"]]"+links));

                        }
        }
	public static class Count2Reduce extends Reducer<Text, Text, Text, Text>{
                public void reduce (Text node, Iterable<Text> scoresFromOther, Context context)
                        throws IOException, InterruptedException {
				Double scoreSum = 0.0;
				String links = "";
                                for(Text scoreFromOther:scoresFromOther){
                                        String scoreitem [] = scoreFromOther.toString().split("\\]\\]"); 
					scoreSum = scoreSum + Double.valueOf(scoreitem[0]);
					for(int i = 1; i< scoreitem.length; i++)
						links = links + "" + scoreitem[i];
                                }
                                context.write(node, new Text(String.valueOf(scoreSum)+"]]"+links));

                        }
        }
	public static class CountReduce extends Reducer<Text, Text, Text, Text>{
                public void reduce (Text node, Iterable<Text> scoresFromOther, Context context)
                        throws IOException, InterruptedException {
                                Double scoreSum ;
                                if(node.toString().equals("[]")){
                                       scoreSum = 0.0;
                                }
                                else
                                        scoreSum= 0.15;
                                String links = "";
                                for(Text scoreFromOther:scoresFromOther){
                                        String scoreitem [] = scoreFromOther.toString().split("\\]\\]");
                                        scoreSum = scoreSum + Double.valueOf(scoreitem[0]);
                                        for(int i = 1; i< scoreitem.length; i++)
                                                links = links + "" + scoreitem[i];
                                }
                                context.write(node, new Text(String.valueOf(scoreSum)+"]]"+links));

                        }
        }
        public static class SortComparator extends WritableComparator {
                protected SortComparator() {
                        super(Text.class, true);
                }
                @Override
                        public int compare(WritableComparable w1, WritableComparable w2) {
                                String termw1[] = ((Text)w1).toString().split("##");
                                String termw2[] = ((Text)w2).toString().split("##");
                                return (Double.valueOf(termw2[1]).compareTo(Double.valueOf(termw1[1])));
                        }
        }
       public static class SortMap extends Mapper<Text, Text, Text, Text>{

                public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
                                String scoreValue[] = value.toString().split("]]");
                                context.write(new Text(key.toString()+"##"+scoreValue[0]), new Text(scoreValue[0])) ;
                }
        }
        public static class SortReduce extends Reducer<Text, Text, Text, Text>{
                public void reduce (Text node, Iterable<Text> scores, Context context)
                        throws IOException, InterruptedException {
                                for(Text score : scores){
                                        String nodeScore [] = node.toString().split("##");
					if(!nodeScore[0].equals("[]"))
						context.write(new Text(nodeScore[0]) , score);
                                }

                        }
        }
	private Job BuildJob(Path inputPath, Path outputPath) throws IOException {

		Job job = new Job(getConf(), "Build");
		job.setJarByClass(getClass());

		// input
		TextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// mapper
		job.setMapperClass(BuildMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// partitioner
		job.setPartitionerClass(BuildPartitioner.class);

		// grouping
		job.setGroupingComparatorClass(BuildGroupComparator.class);

		// reducer
		job.setReducerClass(BuildReduce.class);
//		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setNumReduceTasks();
		// job.setNumMapTasks();

		// output
		TextOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job;
	}
	private Job GraphJob(Path inputPath, Path outputPath) throws IOException {
		Job job = new Job(getConf(), "Graph");
		job.setJarByClass(getClass());

		// input
		TextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// mapper
		job.setMapperClass(GraphMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(GraphPartitioner.class);
	//	job.setCombinerClass(GraphCombine.class);
		// reducer
		job.setReducerClass(GraphReduce.class);
		//job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// output
		TextOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job;
	}

	 private Job CountJob(Path inputPath, Path outputPath) throws IOException {

                Job job = new Job(getConf(), "Count");
                job.setJarByClass(getClass());

                // input
                TextInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                // mapper
                job.setMapperClass(CountMap.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(CountPartitioner.class);
		//job.setCombinerClass(CountCombine.class);

                // reducer
                job.setReducerClass(CountReduce.class);
                //job.setReducerClass(Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);


                // output
                TextOutputFormat.setOutputPath(job, outputPath);
                job.setOutputFormatClass(TextOutputFormat.class);

                return job;
        }
 	private Job Count2Job(Path inputPath, Path outputPath) throws IOException {

                Job job = new Job(getConf(), "Count");
                job.setJarByClass(getClass());

                // input
                TextInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                // mapper
                job.setMapperClass(Count2Map.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                //job.setPartitionerClass(CountPartitioner.class);
                //job.setCombinerClass(CountCombine.class);
                
		// reducer
                job.setReducerClass(Count2Reduce.class);
                //job.setReducerClass(Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);


                // output
                TextOutputFormat.setOutputPath(job, outputPath);
                job.setOutputFormatClass(TextOutputFormat.class);

                return job;
        }
	private Job SortJob(Path inputPath, Path outputPath) throws IOException {

                Job job = new Job(getConf(), "Sort");
                job.setJarByClass(getClass());

                // input
                TextInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                // mapper
                job.setMapperClass(SortMap.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

		job.setSortComparatorClass(SortComparator.class);
                // combiner
                //job.setCombinerClass(invertedCombine.class);

                // partitioner
  //              job.setPartitionerClass(BuildPartitioner.class);

                // grouping
//                job.setGroupingComparatorClass(BuildGroupComparator.class);

                // reducer
                job.setReducerClass(SortReduce.class);
//                job.setReducerClass(Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setNumReduceTasks(1);
                // job.setNumMapTasks();

                // output
                TextOutputFormat.setOutputPath(job, outputPath);
                job.setOutputFormatClass(TextOutputFormat.class);

                return job;
        }
	public void printHelp(Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "PageRank [OPTION]... <INPUTPATH> <OUTPUTPATH>", "Process text files in INPUTPATH and build PageRank Table to OUTPUTPATH.\n", options, "");

	}

	@Override
		public int run(String[] args) throws Exception {

			Options options = new Options();

			options.addOption("graph", false, "build a graph");
			options.addOption("pagerank", false, "pagerank");
			options.addOption("help", false, "this help message.");
			options.addOption("times", false, "add iterative times.");		
	
			CommandLineParser parser = new GnuParser();
			CommandLine cmd ;
			try {
				cmd = parser.parse( options, args);
			}
			catch( ParseException exp ) {
				System.out.println( "Unexpected exception:" + exp.getMessage() );
				printHelp(options);
				return 1;
			}
			if (cmd.hasOption("help") || cmd.getArgs().length < 2 ) { 
				printHelp(options);
				return 1;
			}

			FileSystem fs = FileSystem.get(getConf());
			Path inputPath = new Path(cmd.getArgs()[0]);
			Path output_nocheck_Path = new Path(cmd.getArgs()[1]+"_nocheck");
			Path output_graph_Path = new Path(cmd.getArgs()[1]+ "_graph");
			Path output_tmp1_Path = new Path(cmd.getArgs()[1]+ "_tmp1");
			Path output_tmp2_Path = new Path(cmd.getArgs()[1]+ "_tmp2");
			Path output_sort_Path = new Path(cmd.getArgs()[1]+ "_pagerank");
			int Times =0;
			if(cmd.hasOption("graph")){
				//mapreduce twice, first for no-title-link and dangling node , second for group values of the same key  
//				fs.delete(output_nocheck_Path);
				fs.delete(output_graph_Path);
//				if(BuildJob(inputPath, output_nocheck_Path).waitForCompletion(true)){
					return (GraphJob(output_nocheck_Path, output_graph_Path).waitForCompletion(true) ? 1: 0);
//				}
//				else
//					return 0;
					
			}
			if(cmd.hasOption("pagerank")){
				//run once mapreduce twice, the second is only for root "[]" node
				if(cmd.hasOption("times"))
					Times = Integer.valueOf(cmd.getArgs()[2])-1;
				fs.delete(output_tmp1_Path);
				fs.delete(output_tmp2_Path);
				Path output_pr0_Path = new Path(cmd.getArgs()[1]+ "_pagerank_"+ 0);		
				CountJob(output_graph_Path, output_tmp1_Path).waitForCompletion(true);
				//CountJob(output_graph_Path, output_pr0_Path).waitForCompletion(true); // record all output to several output
				
				for(int i=0; i<Times ;i++){
					fs.delete(output_tmp2_Path);
                                        Count2Job(output_tmp1_Path, output_tmp2_Path).waitForCompletion(true);
                                        fs.delete(output_tmp1_Path);
					CountJob(output_tmp2_Path, output_tmp1_Path).waitForCompletion(true);
				/*	Path input_pr_Path =  new Path(cmd.getArgs()[1]+ "_pagerank_"+ i);
				 	Path output_pr_Path = new Path(cmd.getArgs()[1]+ "_pagerank_"+ (i+1));
					Path output_sort_Path = new Path(cmd.getArgs()[1]+ "_sort_"+ (i+1));
					CountJob(input_pr_Path, output_pr_Path).waitForCompletion(true);
					SortJob(output_pr_Path, output_sort_Path).waitForCompletion(true);
				*/
				}
				fs.delete(output_tmp2_Path);
				Count2Job(output_tmp1_Path, output_tmp2_Path).waitForCompletion(true);

				//sort the pagerank
				fs.delete(output_sort_Path);
				SortJob(output_tmp2_Path, output_sort_Path).waitForCompletion(true);
				return 0;
			}
			return 0;

}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PageRank(), args);
		System.exit(exitCode);
	}

}
