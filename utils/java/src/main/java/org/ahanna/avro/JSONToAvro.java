package org.ahanna.avro.utils;

import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;

import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.data.Json.Writer;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.MappingIterator;

public class JSONToAvro {
	public static void run(String input, String output, String schemaFile) throws IOException {

		// file handles
		File in = new FileReader(input);		
		FileOutputStream outputStream = new FileOutputStream(new File(output));
		Schema schema = new Schema.Parser().parse(new File(schemaFile));

		Decoder decoder = new JsonDecoder(s, in);
        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(JsonNode.class);
        MappingIterator<JsonNode> it = reader.readValues(in);

		Writer w = new Writer();

		while(it.hasNextValue()) {
			w.write( it.nextValue(), e );
        }

		e.flush();
	}

	public static void main(String[] args) {
		try {
			if (args.length < 3) {
				System.err.println("Usage: JSONToAvro <input> <output> <schema>");
				System.exit(0);
			}

			JSONToAvro.run(args[0], args[1], args[2]);
		} catch (IOException e) {
			System.err.println(e);			
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}