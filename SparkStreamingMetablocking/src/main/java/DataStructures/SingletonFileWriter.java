package DataStructures;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class SingletonFileWriter {
	private static SingletonFileWriter uniqueInstance;
	private String path;
	private static long increment;
	 
    private SingletonFileWriter(String path) {
    	this.path = path;
    }
 
    public static synchronized SingletonFileWriter getInstance(String path, long incId) {
        if (uniqueInstance == null)
            uniqueInstance = new SingletonFileWriter(path);
        
        increment = incId;
        return uniqueInstance;
    }
    
    public String save(String text) throws IOException {
    	File arquivo = new File(path.replace("$$", String.valueOf(increment)));
    	String name = arquivo.getAbsolutePath();
        FileOutputStream output;
		output = new FileOutputStream(arquivo, true);//true to append in the end of the file
			
		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(output));
        bufferedWriter.write(text + "\n");
        bufferedWriter.flush();
        output.close();
        bufferedWriter.close();
        
        return name;
    }
}
