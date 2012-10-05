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

public class DateCount extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        static enum Counters { NUM_RECORDS }

        private String mapTaskId;
        private String inputFile;
        
        public void configure(JobConf job) {
            mapTaskId = job.get("mapred.task.id");
            inputFile = job.get("map.input.file");

            Path[] patternFiles = new Path[0];
            try {
                patternFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            
            for (Path f : patternFiles) {
                parsePatternFile(f);
            }                            
        }        
        
        private void parsePatternFile(Path patternFile) {      
            try {
                BufferedReader fis = new BufferedReader(new FileReader(patternFile.toString()));
                String key   = null;
                String value = null;   

                while ((key = fis.readLine()) != null) {
                    // set the first entry as the sum key
                    if (value == null) {
                        value = key;
                    }

                    patternsToKey.put(key, value);
                    patternsToMatch.put(key, Pattern.compile(key, Pattern.CASE_INSENSITIVE));
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + patternFile + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        // convenience method for joining strings like Python
        private String join(Vector<String> v, String sep) {
            boolean first = true;
            StringBuilder sb = new StringBuilder();

            for (String s : v) {
                sb.append(s).append(sep);                    
            }
            return sb.substring(0, sb.length() - 1);
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Vector<String> keyTuple = new Vector<String>();
            Pattern re = Pattern.compile("RT ");

            Gson gson = new Gson();
            HashMap<String, Object> json = gson.fromJson(line, new TypeToken<HashMap<String,Object>>() {}.getType());
            String text       = (String) json.get("text");

            // Parse data in the format of
            // Sat Mar 12 01:49:55 +0000 2011
            SimpleDateFormat strptime = new SimpleDateFormat("EEE MMM dd kk:mm:ss ZZZZZ yyyy");
            SimpleDateFormat strftime = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
            
            StringBuffer sf = strftime.format(
                                              strptime.parse((String) json.get("created_at"),
                                                             new ParsePosition(0)),
                                              new StringBuffer(),
                                              new FieldPosition(0));

            JsonObject user  = (JsonObject) gson.toJsonTree(json.get("user"));

            keyTuple.add((String) json.get("id_str"));
            keyTuple.add(sf.toString());
            keyTuple.add((String) user.get("screen_name").getAsString());
            
            if (user.get("description") == null) {
                keyTuple.add("");
            } else {
                keyTuple.add((String) user.get("description").getAsString());
            }

            if (user.get("location") == null) {
                keyTuple.add("");
            } else {
                keyTuple.add((String) user.get("location").getAsString());
            }

            keyTuple.add((String) json.get("text"));
            
            // try only getting non-retweets, both quoted and not
            if(!json.containsKey("retweeted_status") && !re.matcher(text).matches()) {
                word.set( join(keyTuple, "\t") );
                output.collect(word, one);
            }
            
            // keyTuple.add( sf.toString() );
            // for( String k : patternsToMatch.keySet() ) {
            //     if (patternsToMatch.get(k).matcher(text).find()) {
            //         keyTuple.add( patternsToKey.get( k ) );

            //         // make one big key
            //         word.set( join(keyTuple, "\t") );
            //         output.collect(word, one);                        
                    
            //         // reset the tuple
            //         keyTuple = new Vector<String>();
            //         keyTuple.add( sf.toString() );                    
            //     }                
            // }
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);            
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            // while (values.hasNext()) {
            //     sum += values.next().get();
            // }
            output.collect(key, zero);
        }
    }
 
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(DateCount.class);
        conf.setJobName("datecount");
 
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
 
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

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
        int res = ToolRunner.run(new Configuration(), new DateCount(), args);
        System.exit(res);
    }
}
