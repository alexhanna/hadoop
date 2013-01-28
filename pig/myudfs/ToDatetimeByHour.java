package myudfs;
import java.io.IOException;
import java.text.*;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class ToDatetimeByHour extends EvalFunc<String> {

    public String exec(Tuple input) throws IOException {
    	if (input == null || input.size() == 0) {
        	return null;
        }

        try {
            // Turn data from format 
            // Sat Mar 12 01:49:55 +0000 2011 to 2011-03-12                
            SimpleDateFormat strptime = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");                
            SimpleDateFormat strftime = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                
            strftime.setTimeZone(TimeZone.getTimeZone("GMT+0000"));
            
            StringBuffer sf = strftime.format(
                strptime.parse((String)input.get(0), new ParsePosition(0)), 
            	new StringBuffer(),
            	new FieldPosition(0));

            return sf.toString();
        } catch(Exception e) {
        	throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}