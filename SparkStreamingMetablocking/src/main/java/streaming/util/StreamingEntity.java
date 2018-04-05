package streaming.util;

import java.io.Serializable;
import java.util.Random;

/**
 * This utility class complements the CSVFileStreamGenerator providing a way to parse and represent
 * the key value pairs generated in the CSV files
 */
public class StreamingEntity implements Serializable {

  /**
   * Fill in other fields at random
   * @param key
   */
  public StreamingEntity(Random random, String key) {
    _key = key;
    _value = random.nextInt();
    int whichCat = random.nextInt(8);
    _category = names[whichCat];
  }


  public StreamingEntity(String key, String category, int value) {
    _key = key;
    _category = category;
    _value = value;
  }

  /**
   * Parse out of the format found in generated CSV files
   * @param csvLine A comma separated key and values
   * @throws IllegalArgumentException
   */
  public StreamingEntity(String csvLine) throws IllegalArgumentException {
    String[] parts = csvLine.split(",");
    if (parts.length != 3) {
      throw new IllegalArgumentException("String is not separated by two single commas: " + csvLine);
    }
    _key = parts[0];
    _category = parts[1];
    _value = Integer.valueOf(parts[2]);
  }

  public String getKey() { return _key; }

  public String getCategory() { return _category; }

  public int getValue() { return _value; }

  public String toString() { return _key + "," + _category + "," + _value; }

  private String _key;

  private String _category;

  private int _value;
  
  private String[] names = {"Tiago Brasileiro Araujo", "Diana Simoes Brasileiro", "Polyanna Saraiva", "Lucas Duarte", "Matheus Saraiva", "Ana Cristina Brasil", "Natan Brasileiro", "Danilo Brasileiro Galvao", "Renata Galvao"};
}
