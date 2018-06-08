package evaluation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import DataStructures.EntityProfile;
import DataStructures.GroundTruthEntityIndex;
import DataStructures.IdDuplicates;

public class QualityEvaluation {
	
	public static void main(String[] args) {
		//CHOOSE THE INPUT PATHS
        String INPUT_PATH_GROUNDTRUTH = "inputs/groundtruth_abtbuy";//"inputs/groundtruth_amazongp";//"inputs/groundtruth_abtbuy";
        String INPUT_PATH_BLOCKS = "outputs/abtbyStr2/part-0000";
        int numPart = 1;
        
        
    	HashSet<IdDuplicates> groundtruth = null;
        Map<Integer, List<Integer>> blocks = new HashMap<Integer, List<Integer>>();
        
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
		
    	for (int k = 0; k < numPart; k++) {	

    		BufferedReader br = null;
    		FileReader fr = null;
    		
    		try {
    			//br = new BufferedReader(new FileReader(FILENAME));
    			fr = new FileReader(INPUT_PATH_BLOCKS + k);
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
    				List<Integer> entitiesToCompare = new ArrayList<Integer>();
    				for (int i = 1; i < entities.length; i++) {
    					entitiesToCompare.add(Integer.parseInt(entities[i]));
    				}
    				
    				totalComparisons += entitiesToCompare.size();
    				
    				blocks.put(key, entitiesToCompare);
    				
    				
    			}
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
        
		int duplicadasIdentificadas = 0;
		for (IdDuplicates idDuplicates : groundtruth) {
			List<Integer> listOfEntitiesIntoBlock = blocks.get(idDuplicates.getEntityId1());
			if (listOfEntitiesIntoBlock != null && listOfEntitiesIntoBlock.contains(idDuplicates.getEntityId2())) {
				duplicadasIdentificadas++;
			}
			blocks.remove(idDuplicates.getEntityId1());// avoid compute more than one time
		}
		double pc = (((double)duplicadasIdentificadas)/groundtruth.size());
		double pq = (((double)duplicadasIdentificadas)/totalComparisons);
		System.out.println("PC= " + pc);
		System.out.println("PQ= " + pq);
		System.out.println("FM= " + (2*pc*pq)/(pc+pq));
		
		
	}
}
