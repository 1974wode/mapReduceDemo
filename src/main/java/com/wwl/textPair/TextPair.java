package com.wwl.textPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 自定义TextPair类
public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;
    // 默认的构造函数  有好几个，可以根据输入的参数自动选择
    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }
    public TextPair() {
        set(new Text(), new Text());
    }
    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }
    public TextPair(Text first, Text second) {
        set(first, second);
    }
    public Text getFirst() {
        return first;
    }
    public Text getSecond() {
        return second;
    }
    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    /**
     * 就像针对java语言构造任何值的对象，需要重写java.lang.Object中的hashCode(), equals()和toString()方法
     */

    /**
     * MapReduce需要一个Partitioner把map的输出作为输入分成一块块喂给多个reduce
     * 默认的是HashPartitioner，它是通过对象的hashCode函数进行分割，所以hashCode的好坏决定了分割是否均匀，它是一个关键的方法
     *
     * @return
     */
    //当不使用reletive frequency时采用该hashCode求值方式
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextPair) {
            TextPair tp = (TextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    /**
     * 重写toString方法，作为TextOutputFormat输出格式的输出
     *
     * @return
     */
    @Override
    public String toString() {
        return first + "\t" + second;
    }

    /**
     * 当Textpair被用作健时，需要将数据流反序列化为对象，然后再调用compareTo()方法进行比较。
     * 为了提升效率，可以直接对数据的序列化表示来进行比较
     */
    public static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
        public Comparator() {
            super(TextPair.class);
        }
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                /**
                 * Text对象的二进制表示是一个长度可变的证书，包含字符串之UTF－8表示的字节数以及UTF－8字节本身。
                 * 读取该对象的起始长度，由此得知第一个Text对象的字节表示有多长；然后将该长度传给Text对象RawComparator方法
                 * 最后通过计算第一个字符串和第二个字符串恰当的偏移量，从而实现对象的比较
                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是某个成员的长度
                 */
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                //先比较first
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                //再比较second
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
    // 注册RawComparator, 这样MapReduce使用TextPair时就会直接调用Comparator
    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }
}

