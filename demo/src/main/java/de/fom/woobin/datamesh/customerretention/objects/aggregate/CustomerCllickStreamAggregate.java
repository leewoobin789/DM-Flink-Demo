package de.fom.woobin.datamesh.customerretention.objects.aggregate;

import lombok.Data;

@Data
public class CustomerCllickStreamAggregate {
    public String itemId;
    public int countStreamAggregate;
}