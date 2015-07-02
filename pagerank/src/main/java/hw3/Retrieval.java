package hw3;

import java.util.Collections;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.text.DecimalFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
//import org.apache.hadoop.io.MapFile;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;

public class Retrieval  extends Configured implements Tool {
	public static final int N = 44;

	private static Configuration conf = null;

	// http://hbase.apache.org/0.94/apidocs/

	static {
		conf = HBaseConfiguration.create();
	}
	public static String getRecord(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);

		Get get = new Get(rowKey.getBytes("UTF-8"));
		Result rs = table.get(get);
		String value = "";
		//byte[] value= new byte[rowKey.length];
		for (KeyValue kv : rs.raw()) {
	//		System.out.print(new String(kv.getRow()) + " - ");
	//		System.out.print(new String(kv.getFamily()) + ":");
	//		System.out.print(new String(kv.getQualifier()) + " ");
	//		System.out.print(kv.getTimestamp() + " ");
		//	System.out.println(Bytes.toString(kv.getValue()));
			value = Bytes.toString(kv.getValue());//new String(kv.getValue());
			//return kv.getValue();
		}
		return value;
	}
	public static HashMap<String, Integer> parseQuery(final String query) {

		HashMap<String, Integer> queryMap = new HashMap<String, Integer>();

		int index = 0;
		for (String term : query.split(" +(AND +|OR +)?-?")) {
			queryMap.put(term.toLowerCase(), index++);
		}

		return queryMap;
	}
	public static double tfIdf(final double tf, final double df) {
		return tf * (Math.log(N / df)*0.43);
	}

	public static void printResult(String[] resultArray)throws IOException, UnsupportedEncodingException {
		int i = 0;
		String fileInfoitem[];// filename score term1:offset#,term2:offset#, 
		String offsetsitem[];
		String termsitem[];
		String termitem[];
		String filename;
		String score;
		String term;
		DecimalFormat formatter = new DecimalFormat("#.###");
		//for (FileInfo fileInfo : resultArray) {
		for(String fileInfo : resultArray){
			if ( i == 10) break;
			fileInfoitem = fileInfo.split("##");

			System.out.println("---------------------------------------------------------------");
			filename = fileInfoitem[0];
			score = fileInfoitem[1];
			double scoredouble = Double.valueOf(score);

		
			System.out.printf("Page/%-40s%35s\n",filename , scoredouble);//formatter.format(scoredouble));
			i++;
			/*termsitem = fileInfoitem[2].split(",");    
			for(String terms : termsitem){
				//termitem = terms.split(":");
				//term = termitem[0];
				System.out.println("-" + terms);  
			}
			i++;*/

		}
	}


	public void printHelp(Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "Retrieval [OPTION]... <INVERTED_INDEX_PATH> <DOC_PATH> <QUERY>", "Search words of documents in <doc_path> based on query and calculated inverted_inedx.\n", options, "");
	}


	private CommandLine argsParse(String [] args) {

		Options options = new Options();

		options.addOption("help", false, "this help message.");

		CommandLineParser parser = new GnuParser();
		CommandLine cmd ;
		try {
			cmd = parser.parse( options, args);
		}
		catch( ParseException exp ) {
			System.err.println( "Unexpected exception:" + exp.getMessage() );
			printHelp(options);

			return null;
		}
		if (cmd.hasOption("help") || cmd.getArgs().length < 1) {
			printHelp(options);
			return null;
		}
		return cmd;

	}


	public int run(String[] args) throws Exception {

		CommandLine cmd = argsParse(args);
		if (cmd == null) {
			return 1;
		}
		String query = cmd.getArgs()[0];
		HashMap<String, String> pageRankMap = new HashMap<String, String>();
		String tableNameInverted = "103062505_inverted";
		String tableNamePageRank = "103062505_pagerank";
		String titles = Retrieval.getRecord(tableNameInverted, query);
		String titlePR;
		System.out.println(  "Word  :  \033[34m" + query +"\033[m");
		String titlesItem[] = new String(titles).split("##");
		for(int i = 0; i < titlesItem.length ; i++){
			String title = titlesItem[i];
			//System.out.println(title);
			titlePR = Retrieval.getRecord(tableNamePageRank, title);
			pageRankMap.put(title, new String(titlePR));
		}
		ArrayList ArrayListResult = new ArrayList();
		String resultArray[] = new String[titlesItem.length];
		Map<String, String> map =sortByValues(pageRankMap);
                Set set = pageRankMap.entrySet();
		Set set2 = map.entrySet();
                Iterator iterator = set2.iterator();
                while(iterator.hasNext()){
                                Map.Entry<String,String> me = (Map.Entry)iterator.next();
                                ArrayListResult.add((String)me.getKey()+ "##" + me.getValue());
                }
                for(int i =0; i <ArrayListResult.size();i++){
                                resultArray[i]= String.valueOf(ArrayListResult.get(i));
        //                        System.out.println(resultArray[i]);
                }
		printResult(resultArray);
		return 0;

	}
	private static HashMap<String,String> sortByValues(HashMap map) { 
		List<Map.Entry<String,String>> list = new LinkedList<Map.Entry<String, String>>(map.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator<Map.Entry<String, String>> (){
				public int compare(Map.Entry<String,String> o1, Map.Entry<String,String> o2) {
				Map.Entry<String,String> a = (Map.Entry) o1;
				Map.Entry<String,String> b = (Map.Entry) o2;
				double ad = Double.valueOf(a.getValue());
				double bd = Double.valueOf(b.getValue());
				int c = Double.compare(bd,ad);
				//	System.out.println(ad+ " "+ bd + "= "+c);	
				return c;    
				}
				});
	//	String tmp1;
	//	String tmp2[] = new String[2];
		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap<String,String> sortedHashMap = new LinkedHashMap<String,String>();
		for(Map.Entry<String,String> entry: list){
			//tmp1 = String.valueOf(entry.getKey());

			sortedHashMap.put(entry.getKey(), entry.getValue());
		}      
		// for (Iterator it = list.iterator(); it.hasNext();) {
		//        Map.Entry entry = (Map.Entry) it.next();
		//       sortedHashMap.put(entry.getKey(), entry.getValue());
		// } 
		return sortedHashMap;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Retrieval(), args);
		System.exit(exitCode);
	}
	}
