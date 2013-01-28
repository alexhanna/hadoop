
public class ConvertToSeq {

    public static void main(String[] args) throws IOException,
                                                  InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName("Convert Text");
        job.setJarByClass(Mapper.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        // increase if you need sorting or a special number of files
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path("/lol"));
        SequenceFileOutputFormat.setOutputPath(job, new Path("/lolz"));

        // submit and wait for completion
        job.waitForCompletion(true);
    }
}
