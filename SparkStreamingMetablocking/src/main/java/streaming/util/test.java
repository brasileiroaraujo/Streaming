package streaming.util;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;

public class test {

	public static void main(String[] args) throws InterruptedException {
		//reading the files
//        ObjectInputStream ois;
//		try {
//			ois = new ObjectInputStream(new FileInputStream("C:/Users/Brasileiro/Eclipse Bigsea/eclipse/workspaceStreaming/SparkStreamingMetablocking/inputs/dataset2_gp"));
//			ArrayList<EntityProfile> list = (ArrayList<EntityProfile>) ois.readObject();
////	        while (entity != null) {
////	        	list.add(entity);
////			}
//	        ois.close();
//	        
//
//	        for (EntityProfile voz : list) {
//	        	for (Attribute at : voz.getAttributes()) {
//					System.out.println(at.getName() + ": " + at.getValue());
//				}
//	            System.out.println();
//	        }
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		//end reading
		
//		EntityProfile e1 = new EntityProfile("aaaa;123;y",  ";");
//		EntityProfile e2 = new EntityProfile("aaaa;123;y",  ";");
//		System.out.println(e1.getClass().hashCode());
//		System.out.println(e2.getClass().hashCode());
		ArrayList<Node> nodeList = new ArrayList<Node>();
		nodeList.add(new Node(1));
		nodeList.add(new Node(2));
		nodeList.add(new Node(3));
		nodeList.add(new Node(4));
		
		
		Thread.sleep(2000);
		
		
		long currentTime = System.currentTimeMillis();
		ArrayList<Node> copy = new ArrayList<Node>(nodeList);
		for (Node node : copy) {
			System.out.println(currentTime - node.getStartTime());
			if ((currentTime - node.getStartTime()) > (1*1000)) {//*1000 to convert in miliseconds
				nodeList.remove(node);
			}
		}
		copy = null;
		
		
		for (Node node : nodeList) {
			System.out.println(node.getId());
		}
	}

}
