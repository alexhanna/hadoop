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

public class Frequency extends Configured implements Tool {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        static enum Counters { NUM_RECORDS }

        private String mapTaskId;
        private String inputFile;
        private String mode;
        private int numUsers; 

        private long numRecords = 0;

        private HashMap<String, String> users         = new HashMap<String, String>();
        private HashMap<String, String> userCategory  = new HashMap<String, String>();
        private HashMap<String, Pattern> usersToMatch = new HashMap<String, Pattern>();

        private HashMap<String, String> patternsToKey = new HashMap<String, String>();
        private HashMap<String, Pattern> patternsToMatch = new HashMap<String, Pattern>();

        private Vector<String> locationsOrder = new Vector<String>();
        private HashMap<String, Vector<Pattern>> locationsToMatch = new HashMap<String, Vector<Pattern>>();
        
        public void configure(JobConf job) {
            mapTaskId = job.get("mapred.task.id");
            inputFile = job.get("map.input.file");
            mode      = job.get("frequency.mode");
            
            String userFile  = job.get("frequency.userFilename");           
            String catFile   = job.get("frequency.catFilename");

            if (job.getBoolean("frequency.userFile", true)) {
                Path[] userFiles = new Path[0];
                try {
                    userFiles = DistributedCache.getLocalCacheFiles(job);
                } catch (IOException ioe) {
                    System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
                }
                
                for (Path f : userFiles) {
                    // TK: FIX THIS
                    //if (f.getName().endsWith( userFile )) {
                        parseUserFile(f);
                        //}
                }           
            }
                        
            if (job.getBoolean("frequency.catFile", true)) {
                Path[] userCategories = new Path[0];
                try {
                    userCategories = DistributedCache.getLocalCacheFiles(job);
                } catch (IOException ioe) {
                    System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
                }
                
                for (Path f : userCategories) {
                    if (f.getName().endsWith( catFile )) {
                        parseCategories(f);
                    }
                }           
            }
            
            if (job.getBoolean("frequency.patternFile", true)) {
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
            
            
            if (job.getBoolean("frequency.locationFile", true)) {
                Path[] locationFiles = new Path[0];
                try {
                    locationFiles = DistributedCache.getLocalCacheFiles(job);
                } catch (IOException ioe) {
                    System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
                }
            
                for (Path f : locationFiles) {
                    parseLocationFile(f);
                }                            
            }
        } 
        
        private void parseLocationFile(Path locationFile) {            
            // File is of the form 
            // "city1_1", "city1_2", ... "city1_n"
            // "city2_1", "city2_2", ... "city2_n"

            try {
                BufferedReader fis = new BufferedReader(new FileReader(locationFile.toString()));
                String line        = null;
                String[] words     = null;
                Vector<Pattern> v  = null;
                
                // store in a list to preserve order
                while ((line = fis.readLine()) != null) {
                    words = line.split(",");
                    v     = new Vector<Pattern>();

                    if (words[0] != null) {
                        // add first word to order list
                        locationsOrder.add(words[0]);
                        
                        // add to Pattern Vector
                        for (String s : words) { 
                            v.add(Pattern.compile(s, Pattern.CASE_INSENSITIVE));
                        }

                        locationsToMatch.put(words[0], v);
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + locationFile + "' : " + StringUtils.stringifyException(ioe));
            } catch (Exception e) {
                System.err.println( e.getMessage() );
            }
        }

        private void parseUserFile(Path userFile) {            
            try {
                BufferedReader fis = new BufferedReader(new FileReader(userFile.toString()));
                String user = null;

                while ((user = fis.readLine()) != null) {
                    // user file is separated by level
                    String[] u = user.split("\t");

                    if(u[0] != "user_id") {
                        users.put(u[0], u[1]);
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + userFile + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        private void parseCategories(Path catFile) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(catFile.toString()));
                String user = null;

                while ((user = fis.readLine()) != null) {
                    // user file is separated by level
                    String[] u = user.split("\t");

                    if(u[0] != "user_id") {
                        userCategory.put(u[0], u[1]);
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + catFile + "' : " + StringUtils.stringifyException(ioe));
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
            
            String[] keywords = {
                "trayvon"
            };
            
            for (String k : keywords) {
                patternsToMatch.put(k, Pattern.compile(k, Pattern.CASE_INSENSITIVE));                
            }
            
            try {
                Gson gson = new Gson();
                HashMap<String, Object> json = gson.fromJson(line, new TypeToken<HashMap<String,Object>>() {}.getType());
                String created_at  = (String) json.get("created_at");
                String text        = (String) json.get("text");

                // get the user
                JsonObject user = (JsonObject) gson.toJsonTree(json.get("user"));                

                String user_id = ((JsonPrimitive) user.get("id_str")).getAsString();
                String screen_name = ((JsonPrimitive) user.get("screen_name")).getAsString();

                // String location = "";

                // if (user.get("location") != null) {
                //     location = ((JsonPrimitive) user.get("location")).getAsString();
                // }
                
                // TK: Somehow stuff is getting into this sample in different ways.
                // Need to figure out how.

                // Double dbl      = (Double) json.get("in_reply_to_user_id");
                // String reply_id = null;
                // if (dbl != null) {
                //     reply_id = dbl.toString();
                // }

                // Turn data from format 
                // Sat Mar 12 01:49:55 +0000 2011 to 2011-03-12                
                SimpleDateFormat strptime = new SimpleDateFormat("EEE MMM dd kk:mm:ss ZZZZZ yyyy");
                SimpleDateFormat strftime = new SimpleDateFormat("yyyy-MM-dd kk:mm:00");

                GregorianCalendar gc = new GregorianCalendar();
                gc.setTime(strptime.parse(created_at, new ParsePosition(0)));
                
                StringBuffer sf = strftime.format(gc.getTime(),
                                                  new StringBuffer(),
                                                  new FieldPosition(0));
                String userLevel = users.get( user_id );  
                
                if (mode.equals("volume")) {
                    word.set( sf.toString() );
                    output.collect(word, one);
                } else if (mode.equals("getTweets")) {
                    if (userLevel != null && userLevel.equals("1")) {
                        for( String k : patternsToMatch.keySet() ) {                                
                            if (patternsToMatch.get(k).matcher(text).find()) {
                                keyTuple.add( sf.toString() );
                                keyTuple.add( screen_name );
                                //keyTuple.add( userCategory.get(user_id) );                               
                                keyTuple.add( text );
                                
                                word.set( join(keyTuple, "\t") );
                                output.collect(word, one);
                            } 
                        }
                    } 
                } else if (mode.equals("rtCategorize")) {
                    // get the retweet                    
                    if (json.get("retweeted_status") != null) {
                        JsonObject rt = (JsonObject) gson.toJsonTree(json.get("retweeted_status"));
                    
                        // get RT'd user
                        JsonObject rtUser = (JsonObject) gson.toJsonTree(rt.get("user"));
                        String rtUserId = null;
                        if (rtUser.get("id_str") != null) {
                            rtUserId  = ((JsonPrimitive) rtUser.get("id_str")).getAsString();
                        }
                        String rtUserLevel = users.get( rtUserId );
                        
                        if (userLevel != null && userLevel.equals("1")) {
                            // add first category
                            keyTuple.add( userCategory.get( user_id ) );

                            if (rtUserLevel != null) { 
                                if (rtUserLevel.equals("1")) {
                                    keyTuple.add( userCategory.get( rtUserId ) );
                                } else if (rtUserLevel.equals("2")) {
                                    keyTuple.add( "level2" );
                                }
                            } else {
                                keyTuple.add( "other" );
                            }
                            
                            word.set( join(keyTuple, "\t") );
                            output.collect(word, one);
                        }
                    }
                } else if (mode.equals("userlevelVolume")) {
                    if (userLevel != null) {
                        keyTuple.add( sf.toString() );
                        keyTuple.add( userLevel );

                        word.set( join(keyTuple, "\t") );
                        output.collect(word, one);
                    }
                } else if (mode.equals("keywordLevel")) {                
                    // check if word is in tweet
                    for( String k : patternsToMatch.keySet() ) {                                
                        if (userLevel != null) {
                            if (patternsToMatch.get(k).matcher(text).find()) {                                                
                                //   if(reply_id != null) {
                                //    userLevel = users.get( reply_id );
                                // }                       

                                word.set( sf.toString() + "\t" + userLevel + "\t" + k );
                                output.collect(word, one);                        
                            }
                        } 
                    }                
                } else if (mode.equals("location")) {

                    // looking for location in text
                    // for( String s : locationsOrder ) {
                    //     Vector<Pattern> v = locationsToMatch.get(s); 
                    //     for (Pattern p : v) {
                    //         if (p.matcher(text).find()) {
                    //             keyTuple.add( s );
                            
                    //             word.set( join(keyTuple, "\t") );
                    //             output.collect(word, one);

                    //             // reset the tuple
                    //             keyTuple = new Vector<String>();
                    //             keyTuple.add( sf.toString() );
                    //         }
                    //     }
                    // }

                    // for parsing peoples' location
                    // and seeing if they are acting
                    // outer:
                    // for( String s : locationsOrder ) {
                    //     Vector<Pattern> v = locationsToMatch.get(s); 
                    //     for (Pattern p : v) {
                    //         // first check if they are in Egypt
                    //         if (p.matcher(location).find()) {
                    //             for (String nazl : actions) {                            
                    //                 // if they have some "nazl" in their text, mark it
                    //                 Pattern p_nazl = Pattern.compile(nazl, Pattern.CASE_INSENSITIVE);
                    //                 if (p_nazl.matcher(text).find()) {  
                    //                     word.set( join(keyTuple, "\t") );
                    //                     output.collect(word, one);
                                    
                    //                     break outer;
                    //                 }
                    //             }
                    //         }
                    //     }
                    // }

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
        JobConf job = new JobConf(getConf(), Frequency.class);

        job.setJobName("frequency");
        job.setBoolean("frequency.patternFile", false);
        job.setBoolean("frequency.userFile", false);
        job.setBoolean("frequency.catFile", false);
        job.setBoolean("frequency.locationFile", false);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            if ("-mode".equals(args[i])) {
                job.set("frequency.mode", args[++i]);
            } else if ("-userFile".equals(args[i])) {
                String userFile = args[++i];
                DistributedCache.addCacheFile(new Path(userFile).toUri(), job);
                job.setBoolean("frequency.userFile", true);
                job.set("frequency.userFilename", userFile);
            } else if ("-catFile".equals(args[i])) {
                String catFile = args[++i];
                DistributedCache.addCacheFile(new Path(catFile).toUri(), job);
                job.setBoolean("frequency.catFile", true);
                job.set("frequency.catFilename", catFile);
            } else if ("-patternFiles".equals(args[i])) {
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job);
                job.setBoolean("frequency.patternFile", true);
            } else if ("-locationFiles".equals(args[i])) {
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job);
                job.setBoolean("frequency.locationFile", true);
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Frequency(), args);
        System.exit(res);
    }
}
