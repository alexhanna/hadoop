import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//White, Tom (2012-05-10). Hadoop: The Definitive Guide (Kindle Locations 5375-5384). OReilly Media - A. Kindle Edition. 

public class SequenceFileWriteDemo { 
    public static void main( String[] args) throws IOException { 
        String uri = args[ 0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create( uri), conf);
        Path path = new Path( uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
 
        
       try { 
            writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
            for (int i = 0; i < 100; i ++) { 
                key.set( 100 - i);
                value.set( DATA[ i % DATA.length]);
                System.out.printf("[% s]\t% s\t% s\n", writer.getLength(), key, value); 
                writer.append( key, value); } 
        } finally 
        { 
            IOUtils.closeStream( writer); 
        } 
    } 
}
