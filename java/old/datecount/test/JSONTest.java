
import java.io.*;
import java.util.*;
import java.text.*;

import com.google.gson.*;
import com.google.gson.reflect.*;

public class JSONTest {

    public static void main(String[] args) {
        try {
            BufferedReader bf = new BufferedReader(new FileReader(args[0]));
            String line; 
            Gson gson = new Gson();

            while( (line = bf.readLine()) != null ) {
                HashMap<String, Object> json = gson.fromJson(line, new TypeToken<HashMap<String,Object>>() {}.getType());
            
                String created_at = (String) json.get("created_at");

                // Sat Mar 12 01:49:55 +0000 2011
                SimpleDateFormat strptime = new SimpleDateFormat("EEE MMM dd kk:mm:ss ZZZZZ yyyy");
                SimpleDateFormat strftime = new SimpleDateFormat("yyyy-MM-dd");

                StringBuffer sf = strftime.format(strptime.parse(created_at, new ParsePosition(0)),
                                                  new StringBuffer(),
                                                  new FieldPosition(0));
                    
                System.out.println(sf.toString());

                // String[] localtime = created_at.split(" ");                
                // System.out.println(localtime[1] + " " + localtime[2] + localtime[5]);
            }

        } catch (Exception e) {
            System.err.println( e.getMessage());
        }
    }
}
