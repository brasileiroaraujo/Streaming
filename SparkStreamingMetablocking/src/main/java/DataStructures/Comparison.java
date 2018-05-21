package DataStructures;

import java.io.Serializable;

/**
 * created on 11.02.2010
 * by gap2
 */

public class Comparison implements Serializable {

    private static final long serialVersionUID = 723425435776147L;

    private final boolean cleanCleanER;
    private final int entityId1;
    private final int entityId2;
    private double utilityMeasure;
    private String signature;

    private int sn_position1;
    private int sn_position2;

    private int window;

    public Comparison(boolean ccER, int id1, int id2) {
        cleanCleanER = ccER;
        entityId1 = id1;
        entityId2 = id2;
        utilityMeasure = -1;
    }

    public Comparison(boolean ccER, int id1, int id2, int w) {
        cleanCleanER = ccER;
        entityId1 = id1;
        entityId2 = id2;
        utilityMeasure = -1;
        window = w;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Comparison other = (Comparison) obj;
        if (this.entityId1 != other.getEntityId1()) {
            return false;
        }
        if (this.entityId2 != other.getEntityId2()) {
            return false;
        }
        return true;
    }

    public int getEntityId1() {
        return entityId1;
    }

    public int getEntityId2() {
        return entityId2;
    }

    public double getUtilityMeasure() {
        return utilityMeasure;
    }

    public void setSignature(String sig) {
        signature = sig;
    }

    public String getSignature() {
        return (entityId1 < entityId2) ? Integer.toString(entityId1) + "-" + Integer.toBinaryString(entityId2) : Integer.toString(entityId2) + "-" + Integer.toBinaryString(entityId1);
        //return (entityId1 < entityId2) ? (Integer.toString(entityId1) + "#" + Integer.toString(entityId2)) : (Integer.toString(entityId2) + "#" + Integer.toString(entityId1));
        //return (Integer.toString(entityId1) + "#" + Integer.toString(entityId2));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + this.entityId1;
        hash = 61 * hash + this.entityId2;
        return hash;
    }

    public boolean isCleanCleanER() {
        return cleanCleanER;
    }

    public void setUtilityMeasure(double utilityMeasure) {
        this.utilityMeasure = utilityMeasure;
    }

    public void set_sn_positions(int p1, int p2) {
        sn_position1 = p1;
        sn_position2 = p2;
    }

    public int[] get_sn_positions() {
        return new int[]{sn_position1, sn_position2};
    }

    public int getWindow() {
        return window;
    }

}
