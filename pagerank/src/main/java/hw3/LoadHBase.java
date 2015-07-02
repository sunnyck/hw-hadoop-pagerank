package hw3;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;

public class LoadHBase extends Configured implements Tool{
	private static Configuration conf = null;

	// You can lookup usage of these api from this website. =)
	// http://hbase.apache.org/0.94/apidocs/

	static {
		conf = HBaseConfiguration.create();
	}

	public static void createTable(String tableName, String[] colFamilys) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("Table already exists!");
		} else {
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for(String colFamily: colFamilys){
				tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
			}
			admin.createTable(tableDescriptor);
			
			System.out.println("Create table " + tableName);
		}
	}

	public static void removeTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
            	admin.disableTable(tableName);
            	admin.deleteTable(tableName);
		System.out.println("Remove table " + tableName);
	}

	public static void addRecord(String tableName, String rowKey, String colFamily,
			String qualifier, String value) throws Exception {
		HTable table = new HTable(conf, tableName);
		
		Put put = new Put(rowKey.getBytes("UTF-8"));
            	put.add(colFamily.getBytes("UTF-8"), qualifier.getBytes("UTF-8"),value.getBytes("UTF-8"));
            	table.put(put);
		//System.out.println("Insert record " + rowKey + " to table " + tableName + " ok.");
	}

	public static void deleteRecord(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		
		List<Delete> list = new ArrayList<Delete>();
        	Delete del = new Delete(rowKey.getBytes());
        	list.add(del);
        	table.delete(list);
		System.out.println("Delete record " + rowKey + " from table " + tableName + " ok.");
	}

	public static void getRecord(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		
		Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");	
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " "); 
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}
	
	public static void getAllRecord(String tableName) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r: rs) {
			for (KeyValue kv : r.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		}
	
	}
	public void printHelp(Options options){
        	HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "HBase [OPTION]... <INVERTED_INDEX_PATH> <DOC_PATH> <QUERY>", "Search words of documents in <doc_path> based on query and calculated inverted_inedx.\n", options, "");
    }
	private CommandLine argsParse(String [] args) {

		Options options = new Options();

		options.addOption("help", false, "this help message.");
		options.addOption("delete", false, "delete table.");
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
		if (cmd.hasOption("help") || cmd.getArgs().length < 2) {
			printHelp(options);
			return null;
		}
		return cmd;

	}
	public int  run(String[] args) throws Exception{
		CommandLine cmd = argsParse(args);
		
		FileSystem fs = FileSystem.get(getConf());
        	Path input1Path = new Path(cmd.getArgs()[0]);
        	Path input2Path = new Path(cmd.getArgs()[1]);
        	Path parent1Path = input1Path.getParent();
		Path parent2Path = input2Path.getParent();
        	FileStatus[] status1 = fs.listStatus(input1Path);
		FileStatus[] status2 = fs.listStatus(input2Path);
		String tablename1 = "103062505_pagerank";
                String tablename2 = "103062505_inverted";
                String[] familys1 = { "pr" };
		String[] familys2 = { "iv" };
		
		LoadHBase.createTable(tablename1, familys1);
                LoadHBase.createTable(tablename2, familys2);
		
		if(cmd.hasOption("delete")){
			LoadHBase.removeTable(tablename1);
			LoadHBase.removeTable(tablename2);
			return 0;
		}
		for (int i=0;i<status1.length;i++){

			if (status1[i].isDir() )
				continue;
			try{
			String line1;
			BufferedReader reader1 = new BufferedReader(new InputStreamReader(fs.open(status1[i].getPath())));
			line1 = reader1.readLine();
			while(line1!=null){
				String linedata[] = line1.split("\t");
				String title = linedata[0];
				String score = linedata[1];
				
					LoadHBase.addRecord(tablename1, title, "pr", "pagerank", score);
					line1 = reader1.readLine();
				}
			reader1.close();
			}	
                        catch (java.io.EOFException e) {
                                      e.printStackTrace();
                      	 }
		}
               for (int i=0;i<status2.length;i++){

                        if (status2[i].isDir() )
                                continue;
                        try{
                        String line2;
                        BufferedReader reader2 = new BufferedReader(new InputStreamReader(fs.open(status2[i].getPath())));
                        line2 = reader2.readLine();
                        while(line2!=null){
                                String linedata[] = line2.split("\t");
                                String word = linedata[0];
                                String title = linedata[1];

                                        LoadHBase.addRecord(tablename2, word, "iv", "title", title);
                                        line2 = reader2.readLine();
                        }
                        reader2.close();
                        }
                        catch (java.io.EOFException e) {
                                      e.printStackTrace();
                         }
                }
		/*try{

                                        LoadHBase.addRecord(tablename2, "Gyorgy", "iv", "title", "B<C3><A9>la Bart<C3><B3>k##Chemotaxis##");
                         }
		catch (java.io.EOFException e) {
                                      e.printStackTrace();
                         }*/
		return 0;
	}
	public static void main(String[] args) throws Exception {
        	int exitCode = ToolRunner.run(new LoadHBase(), args);
        	System.exit(exitCode);
    	}
}
