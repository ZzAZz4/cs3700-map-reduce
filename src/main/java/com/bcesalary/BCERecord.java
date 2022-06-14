package com.bcesalary;

public class BCERecord implements Comparable<BCERecord> {
    public final Long position;
    public final Integer salary;

    BCERecord(Long position, Integer salary) {
        this.position = position;
        this.salary = salary;
    }

    @Override
    public int compareTo(BCERecord o) {
        return this.salary.compareTo(o.salary);
    }
}
