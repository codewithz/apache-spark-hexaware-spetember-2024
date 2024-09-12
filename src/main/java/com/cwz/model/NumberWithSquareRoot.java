package com.cwz.model;

import java.io.Serializable;

public class NumberWithSquareRoot implements Serializable {

    int number;
    double squareRoot;


    public NumberWithSquareRoot() {
    }

    public NumberWithSquareRoot(int number, double squareRoot) {
        this.number = number;
        this.squareRoot = squareRoot;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public double getSquareRoot() {
        return squareRoot;
    }

    public void setSquareRoot(double squareRoot) {
        this.squareRoot = squareRoot;
    }

    @Override
    public String toString() {
        return "NumberWithSquareRoot{" +
                "number=" + number +
                ", squareRoot=" + squareRoot +
                '}';
    }
}
