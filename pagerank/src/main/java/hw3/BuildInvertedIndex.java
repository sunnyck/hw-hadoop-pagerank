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


public class BuildInvertedIndex  extends Configured implements Tool {
    private final Logger log = Logger.getLogger(getClass());


public static class invertedMap extends Mapper<LongWritable, Text, Text, Text>{   
        private Text word = new Text();
	private String fileName;
        //protected void setup(Context context) throws IOException, InterruptedException {
        //    fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
        //}

        public void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
     		String delimiter = "\n";
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
		
                while (tokenizer.hasMoreTokens()) {
                       	String aline = tokenizer.nextToken();
            		fileName = aline.substring(aline.indexOf("<title>") + 7, aline.indexOf("</title>"));
			String texttmp = aline.substring(aline.indexOf("<text>") + 6, aline.indexOf("</text>"));
			Scanner sc = new Scanner(texttmp).useDelimiter("[^a-zA-Z]+");
			
                        while (sc.hasNext()) {
				String wordFile = sc.next() + "##" + fileName;
				context.write(new Text(wordFile) ,new Text( fileName));
			}
			
		}
/*
		String line0 = value.toString();

		Scanner sc0 = new Scanner(line0).useDelimiter("\n");
		
		while(sc0.hasNext()){
			
			String titletmp[] = sc0.next().split("(<\\/?title>)");	
            		fileName = titletmp[1];
			String line = titletmp[2];
			Scanner sc = new Scanner(line).useDelimiter("[^a-zA-Z]+");//("[^a-zA-Z]+");
	    		Text word = new Text();
			Text termInfo = new Text();
                	String offsets ="";
                	Text termaddfilename = new Text();

            		while (sc.hasNext()) {
				termaddfilename.set(String.format("%s##%s", sc.next(), fileName));
		
        		       // MatchResult match = sc.match();
				//offsets =  String.valueOf(match.start() + position.get())+"#";
		
				termInfo.set(String.format("%s", fileName  ));
                		//context.write(new TextPairWC(termText, new Text(fileName)), new TermInfo(fileName, 1, offsets));
				context.write(termaddfilename, termInfo);
            		}	
		}
*/
        }
    }

public static class invertedCombine extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text termFile, Iterable<Text> termInfos, Context context) 
	     throws IOException, InterruptedException {
            	int sum = 0;

		Text termInfo = new Text();
		String termaddfilenameitems[] = termFile.toString().split("##");
		String terminfoitems[] ;  
		String offsetitems[];
		String offsets = "";
                String offsetsorted = "";
		
		//for(Text terminfo: termInfos){
		//	terminfoitems = terminfo.toString().split("##"); 
			//sum += Integer.valueOf(terminfoitems[1]);//items[1]=TF

		//}

/*		offsetitems = offsets.split("#");
		for(int i = 0 ; i < offsetitems.length; i++){
			sortedoffsets.add((int)(Integer.valueOf(offsetitems[i])));
		}


            	Collections.sort(sortedoffsets);
		
		for(int i = 0 ;i < offsetitems.length; i++ ) {
			offsetsorted = offsetsorted +  String.valueOf(sortedoffsets.get(i)) + "#";
		}
*/
		termInfo.set(String.format("%s", termaddfilenameitems[1] ));
            	//context.write(termFile, new TermInfo ( termFile.getSecond().toString(), sum, offsets ));
        	context.write(termFile, termInfo);
	}
    }


 //   public static class TermPartitioner extends Partitioner<TextPairWC, TermInfo> {
public static class TermPartitioner extends Partitioner<Text, Text>{ 
       @Override
//        public int getPartition(TextPairWC termFile, TermInfo termInfo, int numPartitions) {
	public int getPartition(Text termFile, Text termInfo, int numPartitions){
    		String termaddfilenameitems[] = termFile.toString().split("##");
	       // return ( termFile.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        	return (termaddfilenameitems[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(Text.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
		String termw1[] = ((Text)w1).toString().split("##");
		String termw2[] = ((Text)w2).toString().split("##");
		return (termw1[0].compareTo(termw2[0]));	
            //return ((Text)w1).getFirst ().compareTo (((Text)w2).getFirst ());
        }
    }

   public static class invertedReduce extends Reducer<Text, Text, Text, Text>{
        public void reduce (Text termFile, Iterable<Text> termInfos, Context context)
	    throws IOException, InterruptedException {
	 	String termitem[] = termFile.toString().split("##");
        	String result = "";
		
                   
            ArrayList<Text> termInfosls = new ArrayList<Text>();
		for(Text termInfo:termInfos){
			result = result + termInfo.toString() + "##";  
			termInfosls.add(termInfo);
		}
            Text [] termInfos_Array = termInfosls.toArray(new Text[termInfosls.size()]);
            IntWritable df = new IntWritable(termInfos_Array.length);
	    
            context.write(new Text(termitem[0]), new Text(result));// new TermInfoArray(termInfos_Array));

        }
    }

    private Job countTfJob(Path inputPath, Path outputPath) throws IOException {

        Job job = new Job(getConf(), "inverted-index");
        job.setJarByClass(getClass());

        // input
        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(invertedMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // combiner
        job.setCombinerClass(invertedCombine.class);

        // partitioner
        job.setPartitionerClass(TermPartitioner.class);

        // grouping
        job.setGroupingComparatorClass (GroupComparator.class);

        // reducer
        job.setReducerClass(invertedReduce.class);
	//job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // job.setNumReduceTasks();
        // job.setNumMapTasks();

        // output
       // if (doTextOutput == true) {
        TextOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
       // } else {
        //    SequenceFileOutputFormat.setOutputPath(job, outputPath);
         //   job.setOutputFormatClass(SequenceFileOutputFormat.class);
       // }

        return job;
    }

    public void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "BuildInvertedIndex [OPTION]... <INPUTPATH> <OUTPUTPATH>", "Process text files in INPUTPATH and build inverted index to OUTPUTPATH.\n", options, "");

    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();
        
       // options.addOption("text", false, "output in text format(only for checking)");
        options.addOption("help", false, "this help message.");

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
        Path outputPath = new Path(cmd.getArgs()[1]);
        fs.delete(outputPath);

        //Boolean doTextOutput = false;
        //if(cmd.hasOption("text"))
         //   doTextOutput = true;

        return (countTfJob(inputPath, outputPath).waitForCompletion(true) ? 1: 0);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BuildInvertedIndex(), args);
        System.exit(exitCode);
    }

}
