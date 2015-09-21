package proj5;


import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.impl.Log4JLogger;


public class MyMapperReducer extends Configured implements Tool {
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;

	static {
		usernameOpt = new Option("u", "username", true, "username");
		passwordOpt = new Option("p", "password", true, "password");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}

	public static class MyMapper extends Mapper<LongWritable,Text,Text,Mutation> {
		private static final String[] eastDiv= new String[]{"76ers","Bobcats","Bucks","Bulls","Cavs","Celtics","Hawks","Knicks","MiamiHeat","Nets","OrlandoMagic","Pacers","Pistons","Raptors","Wizards"};
		private static final String[] westDiv= new String[]{"okcthunder","Nuggets","TrailBlazers","UtahJazz","TWolves","Lakers","Suns","GSWarriors","Clippers","NBAKings","GoSpursGo","Mavs","Hornets","Grizzlies","Rockets"};

		@Override
		public void map(LongWritable key, Text value, Context output) throws IOException {
			String words[] = value.toString().split(",")[1].split("\\s+");
//			String words1 = value.toString().toLowerCase();
			String fileNamewithcsv = ((FileSplit) output.getInputSplit()).getPath().getName();
			System.out.println(fileNamewithcsv);
			String fileNameparts[]=fileNamewithcsv.split("\\.");
			ColumnVisibility columnVisibility=null;
			if(Arrays.asList(eastDiv).contains(fileNameparts[0])){
				columnVisibility=new ColumnVisibility("east");
			}else if(Arrays.asList(westDiv).contains(fileNameparts[0])){
				columnVisibility=new ColumnVisibility("west");
			}
			Text wincolumnFamily=new Text("win");
			Text losecolumnFamily=new Text("lose");
			Text columnQualifier=new Text("20150406");
			int winCount=0;
			int lossCount=0;
			for(String word :words){
				if(word.equalsIgnoreCase("win")){
					winCount++;
				}
				else if(word.equalsIgnoreCase("lose")){
					lossCount++;
				}
			}
			long sys = System.currentTimeMillis();
			Mutation mutation=new Mutation(new Text(fileNameparts[0]));
			Value winval= new Value(((winCount+"").getBytes()));
			mutation.put(wincolumnFamily, columnQualifier, columnVisibility,sys,winval);
			try {
				output.write(null, mutation);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
			Mutation mutation2=new Mutation(new Text(fileNameparts[0]));
			Value loseval= new Value(((lossCount+"").getBytes()));
			mutation2.put(losecolumnFamily, columnQualifier, columnVisibility, sys,loseval);
			try {
				output.write(null, mutation2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
		public  int countMatches(String str, String sub) {
			if (str.isEmpty() || sub.isEmpty()) {
				return 0;
			}
			int count = 0;
			int idx = 0;
			while ((idx = str.indexOf(sub, idx)) != -1) {
				count++;
				idx += sub.length();
			}
			return count;
		}
	}
	public int run(String[] unprocessed_args) throws Exception {
		Parser p = new BasicParser();

		CommandLine cl = p.parse(opts, unprocessed_args);
		String[] args = cl.getArgs();

		String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length != 4) {
			System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 4.");
		}

		Job job = new Job(getConf(), MyMapperReducer.class.getName());
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[2]));

		job.setMapperClass(MyMapper.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, args[3]);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		job.waitForCompletion(true);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new MyMapperReducer(), args);
		System.exit(res);
	}
}
