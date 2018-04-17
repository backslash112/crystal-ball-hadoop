import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class MyMapWritableTest {

    @Test
    public void testContainsKey() {
        MyMapWritable map = new MyMapWritable();
        map.put(new Text("Hi"), new IntWritable(1));
        Assert.assertFalse(map.containsKey("Hi"));
        Assert.assertTrue(map.containsKey(new Text("Hi")));
    }
}
