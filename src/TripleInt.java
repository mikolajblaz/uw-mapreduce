import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TripleInt implements Writable, Comparable<TripleInt> {
    private Integer first;
    private Integer second;
    private Integer third;

    public TripleInt() {}

    public TripleInt(Integer first, Integer second, Integer third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public TripleInt(TripleInt tr) {
        this.first = tr.first;
        this.second = tr.second;
        this.third = tr.third;
    }

    public Integer getFirst() {
        return first;
    }
    public Integer getSecond() {
        return second;
    }
    public Integer getThird() {
        return third;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }
    public void setSecond(Integer second) {
        this.second = second;
    }
    public void setThird(Integer third) {
        this.third = third;
    }

    public void set(Integer first, Integer second, Integer third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TripleInt)) return false;
        TripleInt pair = (TripleInt) o;
        return (pair.first == this.first) && (pair.second == this.second) && (pair.third == this.third);
    }

    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode())
                ^ (third == null ? 0 : third.hashCode());
    }

    @Override
    public int compareTo(TripleInt tr) {
        return first.compareTo(tr.first);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
        out.writeInt(third);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        third = in.readInt();
    }

    public static TripleInt read(DataInput in) throws IOException {
        TripleInt p = new TripleInt();
        p.readFields(in);
        return p;
    }

    @Override
    public String toString() {
        return first.toString() + '\t' + second.toString() + '\t' + third.toString();
    }
}
