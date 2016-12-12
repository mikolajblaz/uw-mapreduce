import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairInt implements Writable, Comparable<PairInt> {
    private Integer first;
    private Integer second;

    public PairInt() {}

    public PairInt(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public PairInt(PairInt pair) {
        this.first = pair.first;
        this.second = pair.second;
    }

    public Integer getFirst() {
        return first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    public void set(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pair)) return false;
        PairInt pair = (PairInt) o;
        return (pair.first == this.first) && (pair.second == this.second);
    }

    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
    }

    @Override
    public int compareTo(PairInt pair) {
        return first.compareTo(pair.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    public static PairInt read(DataInput in) throws IOException {
        PairInt p = new PairInt();
        p.readFields(in);
        return p;
    }

    @Override
    public String toString() {
        return first.toString() + '\t' + second.toString();
    }
}
