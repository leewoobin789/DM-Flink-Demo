package de.fom.woobin.datamesh.inventory.objects.aggregate;

import lombok.Data;

@Data
public class ClickStreamItemStockAggregate {
    public String id;
    public String itemName;
    public int customerClickCount;
    public int stock;
}
