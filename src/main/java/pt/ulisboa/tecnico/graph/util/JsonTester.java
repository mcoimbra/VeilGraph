package pt.ulisboa.tecnico.graph.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Experiment based on Java Gson - Converting json to Java object
 * 
 * @see http://www.studytrails.com/java/json/java-google-json-parse-json-to-java-tree/
 * @author Miguel E. Coimbra
 *
 */
public class JsonTester {
    public static void main(final String[] args)
    {
        final String jsonFile = args[0];
        final StringBuilder jsonSBuilder = new StringBuilder();
        
        try (final BufferedReader br = new BufferedReader(new FileReader(jsonFile))) {
        	String line = null;
        	while ((line = br.readLine()) != null) {
        		jsonSBuilder.append(line.trim());
        	}
        } catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
        
		final String json = jsonSBuilder.toString();
		
        //json.vertices <-- list of vertices which have name and duration properties
		JsonParser parser = new JsonParser();
		// The JsonElement is the root node. It can be an object, array, null or
		// java primitive.
		JsonElement element = parser.parse(json);
		// use the isxxx methods to find out the type of jsonelement. In our
		// example we know that the root object is the Albums object and
		// contains an array of dataset objects
		if (element.isJsonObject()) {
			JsonObject jobDetails = element.getAsJsonObject();
			
			final JsonArray flinkDataflowNodes = jobDetails.getAsJsonArray("vertices");
			
			for (int i = 0; i < flinkDataflowNodes.size(); i++) {
				JsonObject flinkOperator = flinkDataflowNodes.get(i).getAsJsonObject();
				String operatorName = flinkOperator.get("name").getAsString();
				
				//System.out.println(operatorName);
				
				//System.exit(0);
				
				if(operatorName.startsWith("IterationHead(Simple PageRank")) {
					System.out.println(String.format("%s\t%s = %s -> %s", operatorName, flinkOperator.get("duration").getAsString(), flinkOperator.get("start-time").getAsString(), flinkOperator.get("end-time").getAsString()));
				}
				else if (operatorName.startsWith("CoGroup (Add vertices)")) {
					System.out.println(String.format("%s\t%s = %s -> %s", operatorName, flinkOperator.get("duration").getAsString(), flinkOperator.get("start-time").getAsString(), flinkOperator.get("end-time").getAsString()));
				}
				else if (operatorName.startsWith("Join (Join with source)")) {
					System.out.println(String.format("%s\t%s = %s -> %s", operatorName, flinkOperator.get("duration").getAsString(), flinkOperator.get("start-time").getAsString(), flinkOperator.get("end-time").getAsString()));
				}
				else if (operatorName.startsWith("Join (Join with target)")) {
					System.out.println(String.format("%s\t%s = %s -> %s", operatorName, flinkOperator.get("duration").getAsString(), flinkOperator.get("start-time").getAsString(), flinkOperator.get("end-time").getAsString()));
				}
			}
			
			/*
			System.out.println(albums.get("title").getAsString());
			JsonArray datasets = albums.getAsJsonArray("dataset");
			for (int i = 0; i < datasets.size(); i++) {
				JsonObject dataset = datasets.get(i).getAsJsonObject();
				System.out.println(dataset.get("album_title").getAsString());
			}
			*/
		}
    }
}
