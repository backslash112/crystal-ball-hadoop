import java.io.*;
import org.apache.hadoop.io.*;

public class StringPair implements WritableComparable<StringPair> {

    private String first;
    private String second;


    public StringPair() {

    }
    public StringPair(String first, String second) {
        set(first, second);
    }

    public void set(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeUTF(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readUTF();
    }

    public int compareTo(StringPair o) {
        return this.first.compareTo(o.getFirst());
    }

    @Override
    public String toString() {
        return "(" + this.first + ", " + this.second + ")";
    }
}
