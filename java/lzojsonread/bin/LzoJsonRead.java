package org.ahanna;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.text.*;

import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class LzoJsonRead extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private String mapTaskId;
        private String inputFile;

        private HashMap<String, String> patternsToKey = new HashMap<String, String>();
        private HashMap<String, Pattern> patternsToMatch = new HashMap<String, Pattern>();
        
        // public void configure(JobConf job) {
        //     mapTaskId = job.get("mapred.task.id");
        //     inputFile = job.get("map.input.file");

        //     Path[] patternFiles = new Path[0];
        //     try {
        //         patternFiles = DistributedCache.getLocalCacheFiles(job);
        //     } catch (IOException ioe) {
        //         System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
        //     }
            
        //     for (Path f : patternFiles) {
        //         parsePatternFile(f);
        //     }                            
        // }        
        
        // private void parsePatternFile(Path patternFile) {      
        //     try {
        //         BufferedReader fis = new BufferedReader(new FileReader(patternFile.toString()));
        //         String key   = null;
        //         String value = null;   

        //         while ((key = fis.readLine()) != null) {
        //             // set the first entry as the sum key
        //             if (value == null) {
        //                 value = key;
        //             }

        //             patternsToKey.put(key, value);
        //             patternsToMatch.put(key, Pattern.compile(key, Pattern.CASE_INSENSITIVE));
        //         }
        //     } catch (IOException ioe) {
        //         System.err.println("Caught exception while parsing the cached file '" + patternFile + "' : " + StringUtils.stringifyException(ioe));
        //     }
        // }

        // convenience method for joining strings like Python
        private String join(Vector<String> v, String sep) {
            boolean first = true;
            StringBuilder sb = new StringBuilder();

            for (String s : v) {
                sb.append(s).append(sep);                    
            }
            return sb.substring(0, sb.length() - 1);
        }
        
        public void map(LongWritable key, MapWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            
            java.util.Map.Entry<Writable, Writable> entry = value.entrySet();
                
            // Let's assume that our JSON objects consist of key: count pairs, where key is a string and count is an integer.                
            String created_at = ((Text) entry.getKey( new Text("created_at") )).toString();
            
            // Parse data in the format of Sat Mar 12 01:49:55 +0000 2011
            SimpleDateFormat strptime = new SimpleDateFormat("EEE MMM dd kk:mm:ss ZZZZZ yyyy");
            SimpleDateFormat strftime = new SimpleDateFormat("yyyy-MM-dd kk:00");            
            
            StringBuffer sf = strftime.format(strptime.parse(created_at,
                                                             new ParsePosition(0)),
                                              new StringBuffer(),
                                              new FieldPosition(0));
            
            word.add(created_at, one);
                
                
                //             for (Map.Entry<Writable, Writable> entry: value.entrySet()) {
            //     word_.set((Text)entry.getKey());
            //     Text strValue = (Text)entry.getValue();
                
            //     count_.set(Long.parseLong(strValue.toString()));
            //     context.write(word_, count_);
            // }
            
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
        JobConf conf = new JobConf(getConf());
        conf.setJobName("lzojsonread");
 
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
 
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
 
        // Use the custom LzoTextInputFormat class.
        conf.setInputFormatClass(LzoJsonInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            if ("-patternFiles".equals(args[i])) {
                String[] files = args[++i].split(",");
                for (String f : files) {
                    DistributedCache.addCacheFile(new Path(f).toUri(), conf);
                }
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
  
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LzoJsonRead(), args);
        System.exit(res);
    }
}
