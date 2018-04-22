import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Writable, Writable> item: this.entrySet()) {
            Text key = (Text)item.getKey();
            IntWritable value = (IntWritable)item.getValue();
            sb.append("(");
            sb.append(key.toString());
            sb.append(":");
            sb.append(value.toString());
            sb.append("), ");
        }
        sb.deleteCharAt(sb.length()-2);
        return sb.toString();
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }
}
