package de.fom.woobin.datamesh.inventory.objects.stock;

import lombok.Data;

@Data
public class ItemStock {
    public String id;
    public String itemName;
    public int stock;
    public String updatedTs;
}
