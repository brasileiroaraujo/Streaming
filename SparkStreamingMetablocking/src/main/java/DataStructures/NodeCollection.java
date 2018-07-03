package DataStructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NodeCollection implements Serializable{
	private static final long serialVersionUID = 466473482117035453L;
	
	List<Node> nodeList = new ArrayList<Node>();

	public List<Node> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<Node> nodeList) {
		this.nodeList = nodeList;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	public void add(Node node) {
		this.nodeList.add(node);
	}

	public void blackList() {
		nodeList = new ArrayList<Node>();
		Node blackNode = new Node();
		blackNode.setBlackFlag(true);
		nodeList.add(blackNode);
	}
	
	public boolean isOnTheBlackList() {
		if (!nodeList.isEmpty()) {
//			System.out.println();
			if (nodeList.get(0).isBlackFlag()) {
				return true;
			}
		}
			
		return false;
	}

	public void removeOldNodes(int timeThreshold) {
		long currentTime = System.currentTimeMillis();
		ArrayList<Node> copy = new ArrayList<Node>(nodeList);
		for (Node node : copy) {
			if ((currentTime - node.getStartTime()) > (timeThreshold*1000)) {//*1000 to convert in miliseconds
				nodeList.remove(node);
			}
		}
		copy = null;
	}
	
	

}
