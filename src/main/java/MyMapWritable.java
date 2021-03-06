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

    public void addAll(MyMapWritable map) {
        for (Map.Entry item: map.entrySet()) {
            if (this.containsKey(item.getKey())) {
                IntWritable originValue = (IntWritable)this.get(item.getKey());
                int newValue = originValue.get() + ((IntWritable)item.getValue()).get();
                this.put((Text)item.getKey(), new IntWritable(newValue));
            } else {
                this.put((Text)item.getKey(), (IntWritable)item.getValue());
            }
        }
    }

    public int getTotal() {
        int total = 0;
        for (Map.Entry<Writable, Writable> item : this.entrySet()) {
            int value = ((IntWritable) item.getValue()).get();
            total += value;
        }
        return total;
    }
}
