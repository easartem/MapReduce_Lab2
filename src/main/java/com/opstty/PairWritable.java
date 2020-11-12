package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements Writable
{
    private IntWritable first;
    private IntWritable second;

    // Constructors
    public PairWritable()
    {
        this.first = new IntWritable(0);
        this.second = new IntWritable(0);
    }

    public PairWritable(IntWritable firsty, IntWritable secondy)
    {
        this.first = firsty;
        this.second = secondy;
    }

    // Getters

    public IntWritable getFirst() { return first; }

    public IntWritable getSecond() { return second; }

    // Setters

    public void setFirst(IntWritable first) {
        this.first = first;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
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

}
