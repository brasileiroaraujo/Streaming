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
	
	

}
