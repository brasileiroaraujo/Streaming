package evaluation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import DataStructures.EntityProfile;
import DataStructures.GroundTruthEntityIndex;
import DataStructures.IdDuplicates;
import scala.Tuple2;
import scala.annotation.meta.field;

public class IncrementalQualityEvaluation {
	
	public static void main(String[] args) {
		//CHOOSE THE INPUT PATHS
        String INPUT_PATH_GROUNDTRUTH = "inputs/groundtruth_dblpacm";//"inputs/groundtruth_amazongp";//"inputs/groundtruth_abtbuy";
        String INPUT_PATH_BLOCKS = "C:\\Users\\lutibr\\Documents\\outputs\\imdb_dbpedia_wind120\\";
        
        
    	HashSet<IdDuplicates> groundtruth = null;
        Map<Integer, Set<Integer>> blocks = new HashMap<Integer, Set<Integer>>();
        
		// reading the files
		ObjectInputStream ois1;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH_GROUNDTRUTH));
			groundtruth = (HashSet<IdDuplicates>) ois1.readObject();
			ois1.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int totalComparisons = 0;
		
		File f = new File(INPUT_PATH_BLOCKS);
		
		List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> intervalos = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>();
		
    	for (String fileNames : f.list()) {
    		
    		int indexMinSource = Integer.MAX_VALUE;
    		int indexMaxSource = Integer.MIN_VALUE;
    		
    		int indexMinTarget = Integer.MAX_VALUE;
    		int indexMaxTarget = Integer.MIN_VALUE;

    		BufferedReader br = null;
    		FileReader fr = null;
    		
    		try {
    			//br = new BufferedReader(new FileReader(FILENAME));
    			fr = new FileReader(INPUT_PATH_BLOCKS+fileNames);//INPUT_PATH_BLOCKS + k + ".txt");
    			br = new BufferedReader(fr);
    			String sCurrentLine;
    			while ((sCurrentLine = br.readLine()) != null) {
    				sCurrentLine = sCurrentLine.replace("(", "");
    				sCurrentLine = sCurrentLine.replace(")", "");
    				sCurrentLine = sCurrentLine.replace("[", "");
    				sCurrentLine = sCurrentLine.replace("]", "");
    				sCurrentLine = sCurrentLine.replace(" ", "");
    				String[] entities = sCurrentLine.split(",");
    				
    				//old version of prime
//    				if (entities[0].contains("S")) {
//    					int key = Integer.parseInt(entities[0].replace("S", ""));
//    					List<Integer> entitiesToCompare = new ArrayList<Integer>();
//    					for (int i = 1; i < entities.length; i++) {
//    						entitiesToCompare.add(Integer.parseInt(entities[i].replace("T", "")));
//    					}
//    					
//    					totalComparisons += entitiesToCompare.size();
//    					
//    					blocks.put(key, entitiesToCompare);
//    				}
    				
    				//new version
    				int key = Integer.parseInt(entities[0]);
    				
    				if (key > indexMaxSource) {
    					indexMaxSource = key;
					}
    				if (key < indexMinSource) {
    					indexMinSource = key;
					}
    				
    				Set<Integer> entitiesToCompare = new HashSet<Integer>();
    				for (int i = 1; i < entities.length; i++) {
    					if (Integer.parseInt(entities[i]) > indexMaxTarget) {
        					indexMaxTarget = Integer.parseInt(entities[i]);
    					}
        				if (Integer.parseInt(entities[i]) < indexMinTarget) {
        					indexMinTarget = Integer.parseInt(entities[i]);
    					}
    					entitiesToCompare.add(Integer.parseInt(entities[i]));
    				}
    				
    				if (blocks.containsKey(key)) {
    					entitiesToCompare.addAll(blocks.get(key));
    					blocks.put(key, entitiesToCompare);
					} else {
						blocks.put(key, entitiesToCompare);
					}
    				
    				
    				
    			}
    			
    			intervalos.add(new Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>(new Tuple2<Integer,Integer>(indexMinSource, indexMaxSource), new Tuple2<Integer,Integer>(indexMinTarget, indexMaxTarget)));
    		} catch (IOException e) {
    			e.printStackTrace();
    		} finally {
    			try {
    				if (br != null)
    					br.close();
    				if (fr != null)
    					fr.close();
    			} catch (IOException ex) {
    				ex.printStackTrace();
    			}

    		}
    		
		}
    	
    	for (Integer key : blocks.keySet()) {
    		totalComparisons += blocks.get(key).size();
		}
    	
    	//clean (based on the intervals) the groundtruth
    	HashSet<IdDuplicates> groundtruthCleaned = new HashSet<IdDuplicates>();
    	for (IdDuplicates initDup : groundtruth) {
    		int e1 = initDup.getEntityId1();
    		int e2 = initDup.getEntityId2();
    		for (Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> tuple2 : intervalos) {
    			if (intervall(tuple2._1()._1(), tuple2._1()._2(), e1) && intervall(tuple2._2()._1(), tuple2._2()._2(), e2)) {
    				groundtruthCleaned.add(initDup);
				}
    		}
		}
    	
        
		int duplicadasIdentificadas = 0;
		int groundtruthSize = groundtruthCleaned.size();
		HashSet<IdDuplicates> groundtruthMatched = new HashSet<IdDuplicates>();
		
		for (IdDuplicates idDuplicates : groundtruthCleaned) {
			Set<Integer> listOfEntitiesIntoBlock = blocks.get(idDuplicates.getEntityId1());
			if (listOfEntitiesIntoBlock != null && listOfEntitiesIntoBlock.contains(idDuplicates.getEntityId2())) {
				duplicadasIdentificadas++;
				groundtruthMatched.add(idDuplicates);
			}
			blocks.remove(idDuplicates.getEntityId1());// avoid compute more than one time
		}
		
		//System.out.println(groundtruthMatched.size() + "   " + groundtruthSize);
		for (IdDuplicates idDuplicates : groundtruthCleaned) {
			if (!groundtruthMatched.contains(idDuplicates)) {
				System.out.println(idDuplicates.getEntityId1() + "-" + idDuplicates.getEntityId2());
			}
		}
		System.out.println(intervalos);
		double pc = (((double)duplicadasIdentificadas)/groundtruthSize);
		double pq = (((double)duplicadasIdentificadas)/totalComparisons);
		System.out.println("PC= " + pc);
		System.out.println("PQ= " + pq);
		System.out.println("FM= " + (2*pc*pq)/(pc+pq));
		
		
	}
	
	public static boolean intervall(int low, int high, int n) {
	    return n >= low && n <= high;
	}
}
