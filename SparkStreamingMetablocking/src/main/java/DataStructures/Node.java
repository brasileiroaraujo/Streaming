package DataStructures;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.Tuple2;
import scala.noinline;
import scala.annotation.elidable;

public class Node implements Serializable {
	private int id;
	private Set<Integer> blocks;
	private Set<Tuple2<Integer, Double>> neighbors;
	private boolean isSource;
	private boolean marked;
	private double sumWeight = 0.0;
	private int numberOfNeighbors = 0;
	
	
	public Node(int id, Set<Integer> blocks, Set<Tuple2<Integer, Double>> neighbors, boolean isSource) {
		super();
		this.id = id;
		this.blocks = blocks;
		this.neighbors = neighbors;
		this.isSource = isSource;
	}


	public Node() {
		this.id = -1;
	}


	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public Set<Integer> getBlocks() {
		return blocks;
	}


	public void setBlocks(Set<Integer> blocks) {
		this.blocks = blocks;
	}


	public Set<Tuple2<Integer, Double>> getNeighbors() {
		return neighbors;
	}


	public void setNeighbors(Set<Tuple2<Integer, Double>> neighbors) {
		this.neighbors = neighbors;
	}
	
	public void addNeighbor(Tuple2<Integer, Double> neighbor) {
		this.sumWeight += neighbor._2();
		this.numberOfNeighbors ++;
		this.neighbors.add(neighbor);
	}
	
	public void addSetNeighbors(Node n2) {
		this.sumWeight += n2.sumWeight;
		this.numberOfNeighbors += n2.numberOfNeighbors;
		this.neighbors.addAll(n2.getNeighbors());
	}
	
	public void pruning() {
		double threshold = sumWeight/numberOfNeighbors;
		Set<Tuple2<Integer, Double>> prunnedNeighbors = new HashSet<>();
		
		for (Tuple2<Integer, Double> neighbor : neighbors) {
			if (neighbor._2() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	public boolean isMarked() {
		return marked;
	}


	public void setMarked(boolean marked) {
		this.marked = marked;
	}


	@Override
	public String toString() {
//		String output = id + ":";
		String output = "";
		for (Tuple2<Integer, Double> neighbor : neighbors) {
			output += neighbor._1() + ",";
		}
		if (!neighbors.isEmpty()) {
			return output.substring(0,output.length()-1);
		} else {
			return output;
		}
		
	}
	
	
}
