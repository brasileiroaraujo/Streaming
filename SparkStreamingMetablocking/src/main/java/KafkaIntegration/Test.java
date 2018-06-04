package KafkaIntegration;

import java.util.Set;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class Test {
	public static void main(String[] args) {
//		String content = "Sony Turntable - PSLX350H/ Belt Drive System/ 33-1/3 and 45 RPM Speeds/ Servo Speed Control/ Supplied Moving Magnet Phono Cartridge/ Bonded Diamond Stylus/ Static Balance Tonearm/ Pitch Control"; //"They Licked the platter clean";
//		
//		String[] x = gr.demokritos.iit.jinsect.utils.splitToWords(content);
//        KeywordGenerator kw = new KeywordGeneratorImpl();
//        Set<String> kws = kw.generateKeyWords(content);
//        
//        
//		for (String string : x) {
//			System.out.print(string + " ");
//		}
//		
//		System.out.println();
//		
//		for (String string : kws) {
//			System.out.print(string + " ");
//		}
		String s = "Tiago";
		int code = s.hashCode();
		System.out.println(code);
		
	}
	
	private static final int OFFSET = "AAAAAAA".hashCode();

	private static String getStringForHashCode(int hash) {
        hash -= OFFSET;
        // Treat it as an unsigned long, for simplicity.
        // This avoids having to worry about negative numbers anywhere.
        long longHash = (long) hash & 0xFFFFFFFFL;
        System.out.println(longHash);

        char[] c = new char[7];
        for (int i = 0; i < 7; i++)
        {
            c[6 - i] = (char) ('A' + (longHash % 31));
            longHash /= 31;
        }
        return new String(c);
    }
}
