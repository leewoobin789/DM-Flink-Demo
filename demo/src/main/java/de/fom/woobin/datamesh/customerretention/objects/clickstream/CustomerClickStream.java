package de.fom.woobin.datamesh.customerretention.objects.clickstream;

import lombok.Data;

@Data
public class CustomerClickStream {
    public String userName;
    public String itemOfPage;
    public long ts;
}
