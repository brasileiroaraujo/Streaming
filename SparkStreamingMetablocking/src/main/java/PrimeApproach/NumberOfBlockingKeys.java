package PrimeApproach;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class NumberOfBlockingKeys {
	public static void main(String[] args) {
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		KeywordGenerator kw = new KeywordGeneratorImpl();
		ArrayList<EntityProfile> EntityListSource = null;
		ArrayList<EntityProfile> EntityListTarget = null;
		
		try {
			ois1 = new ObjectInputStream(new FileInputStream("inputs/dataset1_imdb"));
			ois2 = new ObjectInputStream(new FileInputStream("inputs/dataset2_dbpedia"));
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Set<String> keys = new HashSet<String>();
		for (EntityProfile e : EntityListSource) {
			for (Attribute a : e.getAttributes()) {
				keys.addAll(kw.generateKeyWords(a.getValue()));
			}
		}
		
		for (EntityProfile e : EntityListTarget) {
			for (Attribute a : e.getAttributes()) {
				keys.addAll(kw.generateKeyWords(a.getValue()));
			}
		}
		
		System.out.println(keys.size());
	}
}
