package net.jackw.olep.view;

/**
 * A mutable heap-based integer, to store the stock level in the warehouse
 */
public class WarehouseItemStock {
    private int stock;

    public WarehouseItemStock() { }

    public void setStock(int stock) {
        this.stock = stock;
    }

    public int getStock() {
        return stock;
    }

    @Override
    public String toString() {
        return Integer.toString(stock);
    }
}
