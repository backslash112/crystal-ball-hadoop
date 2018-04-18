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

    @Test
    public void testAddAll() {
        MyMapWritable map = new MyMapWritable();
        map.put(new Text("K1"), new IntWritable(1));

        MyMapWritable map2 = new MyMapWritable();
        map2.put(new Text("K2"), new IntWritable(2));

        map.addAll(map2);

        Assert.assertTrue(map.containsKey(new Text("K2")));
        Assert.assertTrue(map.entrySet().size() == 2);


        // Add a new map with same item key
        MyMapWritable map3 = new MyMapWritable();
        map3.put(new Text("K1"), new IntWritable(3));

        map.addAll(map3);
        Assert.assertTrue(map.entrySet().size() == 2);
        Assert.assertTrue(map.get(new Text("K1")) != null);
        Assert.assertTrue(((IntWritable)map.get(new Text("K1"))).get() == 1+3);

    }
}
