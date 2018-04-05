package streaming.util;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

import DataStructures.EntityProfile;

/**
 * This utility class complements the CSVFileStreamGenerator providing a way to
 * parse and represent the key value pairs generated in the CSV files
 */
public class StreamingEntityPMSD implements Serializable {
	private ArrayList<EntityProfile> EntityList;
	private EntityProfile entityStreaming;

	/**
	 * Fill in other fields at random
	 * 
	 * @param key
	 */
	public StreamingEntityPMSD(String fileInput, Random random, String key) {
		// reading the files
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(new FileInputStream(fileInput));
			EntityList = (ArrayList<EntityProfile>) ois.readObject();
			ois.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// end reading
		
		int whichCat = random.nextInt(8);
		EntityProfile entityStreaming = EntityList.get(whichCat);
	}

//	public StreamingEntity(String key, String category, int value) {
//		_key = key;
//		_category = category;
//		_value = value;
//	}

	/**
	 * Parse out of the format found in generated CSV files
	 * 
	 * @param csvLine
	 *            A comma separated key and values
	 * @throws IllegalArgumentException
	 */

}

