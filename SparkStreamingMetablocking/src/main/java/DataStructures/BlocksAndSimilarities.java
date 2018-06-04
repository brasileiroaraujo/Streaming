package DataStructures;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

public class BlocksAndSimilarities implements Serializable {
	private static final long serialVersionUID = -123942050890544812L;
	
	private Tuple2<String, Double> similarity;
	private List<String> blocks;
	
	public BlocksAndSimilarities(Tuple2<String, Double> similarity, List<String> blocks) {
		super();
		this.similarity = similarity;
		this.blocks = blocks;
	}
	public Tuple2<String, Double> getSimilarity() {
		return similarity;
	}
	public void setSimilarity(Tuple2<String, Double> similarity) {
		this.similarity = similarity;
	}
	public List<String> getBlocks() {
		return blocks;
	}
	public void setBlocks(List<String> blocks) {
		this.blocks = blocks;
	}
	
	

}
