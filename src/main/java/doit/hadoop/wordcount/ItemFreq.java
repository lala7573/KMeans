package doit.hadoop.wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 10.
 * Time: 오전 6:50
 * To change this template use File | Settings | File Templates.
 */
public class ItemFreq implements WritableComparable<ItemFreq> {
    private String item;
    private Long freq;

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, item);
        out.writeLong(freq);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        item = WritableUtils.readString(in);
        freq = in.readLong();

    }

    @Override
    public int compareTo(ItemFreq o){
        int result = item.compareTo(o.item);
        if ( 0 == result){
            result = (int) (freq - (o.freq));
        }
        return result;
    }
}
