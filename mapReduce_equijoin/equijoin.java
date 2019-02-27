import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class equijoin {

	public static class Map 
	extends MapReduceBase 
	implements Mapper<Object, Text, Text, Text> 
	{
		private Text joinkey = new Text();
		private Text Finalvalue = new Text();
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter r) 
			   throws IOException 
		{
			
			
			String joinvalue = value.toString();
			String[] newsplit = joinvalue.split(",");
			String joincolumnkey = newsplit[1].trim();
			joinkey.set(joincolumnkey);
			Finalvalue.set(joinvalue);
			output.collect(joinkey, Finalvalue);
		}
	}


		public static class Reduce 
		extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> 
		{
			public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter r)
				   throws IOException 
			{
				Text result = new Text();
				String[] rows = new String[10000];				
				int counter = 0;

				while(values.hasNext())
				{
					rows[counter++] = values.next().toString();
				}
				for(int b = 0; b < counter; b++) 
				{  	
					String[] value = rows[b].split(",");
					
						for(int i = b + 1 ; i < counter; i++) 
						{
							StringBuilder  outputValue = new StringBuilder();
							StringBuilder  outputkey = new StringBuilder();					
							String[] nextval = rows[i].split(",");
							if(!(nextval[0].trim()).equals(value[0].trim())) 
							{
								outputkey.append(rows[b]);
								//outputkey.append(" ,");
								outputValue.append(" "+rows[i]);
								//result.set(outputkey.toString()+outputValue.toString());
								//output.collect(new Text(" "), result);
								output.collect(new Text(outputkey.toString()), new Text(outputValue.toString()));
							}
						}
				 }
			}
		}


	
	public static void main(String[] args) 
			throws Exception
    { 	
    	JobConf job = new JobConf(equijoin.class);
		job.setJobName("equijoin");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.set("mapred.textoutputformat.separator",",");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileSystem fs = FileSystem.get(job);
		if(fs.exists(new Path(args[1])))
		{

			   fs.delete(new Path(args[1]),true);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		JobClient.runJob(job);
    }
}
