

package com.osura.wordcount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class WordCount extends Configured implements Tool {
	
	public static class MapClass extends
			Mapper<Object, Text, Text, IntWritable> {

		//count is always one for every word the mapper finds
		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			//splits the string to words/token
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {

				//every token is a word
				String token = tokenizer.nextToken();
				word.set(token);

				//pass to the reducer every word with count 1
				context.write(word, ONE);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		private IntWritable count = new IntWritable();
		// hashmap to store all previous reducer results
		HashMap<String, Integer> hmap = new HashMap<String, Integer>();

		//to store total count of words
		IntWritable totalcount = new IntWritable(0);



		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {


			System.out.println("totalcount+"+totalcount);
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			count.set(sum);

			hmap.put(key.toString(),sum);
			System.out.println("list size"+hmap.size());

			totalcount.set(totalcount.get()+count.get());


			Set set = hmap.entrySet();
			Iterator iterator = set.iterator();

			//add this key as a delimiter so the final result is easy to filter
			context.write(new Text("----------avg_iterator_start--------"),new FloatWritable(0));
			while(iterator.hasNext()) {
				Map.Entry mentry = (Map.Entry)iterator.next();
				System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
				System.out.println(mentry.getValue());


				System.out.println((float) Integer.parseInt(mentry.getValue().toString())/(float) totalcount.get());
				context.write(new Text(mentry.getKey().toString()),new FloatWritable((float) Integer.parseInt(mentry.getValue().toString())/(float) totalcount.get()));

			}


			context.write(new Text("-----------avg_iterator_end---------"),new FloatWritable(0));



		}
	}





	public int run(String[] arg0) throws Exception {		
        Job job = new Job(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));


		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1; 
	}

}
