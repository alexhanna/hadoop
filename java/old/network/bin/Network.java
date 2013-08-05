package org.ahanna;

import com.google.gson.*; 
import com.google.gson.reflect.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.text.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Network extends Configured implements Tool {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        static enum Counters { NUM_RECORDS }

        private String mapTaskId;
        private String inputFile;

        private long numRecords = 0;
        
        public void configure(JobConf job) {}

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Vector<String> keyTuple = new Vector<String>(); 

            try {
                Gson gson = new Gson();
                HashMap<String, Object> json = gson.fromJson(line, new TypeToken<HashMap<String,Object>>() {}.getType());

                // get the user
                JsonObject user = (JsonObject) gson.toJsonTree(json.get("user"));
                String user1    = ((JsonPrimitive) user.get("id_str")).getAsString();
                
                // get the links to other users
                JsonObject entities = (JsonObject) gson.toJsonTree(json.get("entities"));
                JsonArray ums       = entities.getAsJsonArray("user_mentions");

                for(JsonElement e : ums) {
                    JsonObject o = (JsonObject) e;
                    String user2 = ((JsonPrimitive) o.get("id_str")).getAsString();
                    
                    word.set( user1 + "\t" + user2 );
                    output.collect(word, one);
                }
                                
                reporter.incrCounter(Counters.NUM_RECORDS, 1);
                if(++numRecords % 100 == 0) {
                    reporter.setStatus(mapTaskId + " processed " + numRecords + " from input-file: " + inputFile); 
                }
            } catch (JsonSyntaxException e) {
                System.err.println(e.getMessage());
            }
        }
    }
 
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
 
    public int run(String[] args) throws Exception {
        JobConf job = new JobConf(getConf(), Network.class);

        job.setJobName("network");
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }

        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Network(), args);
        System.exit(res);
    }
}
