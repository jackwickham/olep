package net.jackw.olep.view;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonDeserializer;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.view.records.Customer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RedisAdapter extends KeyValueStoreAdapter {
    private JedisPool pool;

    private JsonSerde<Customer> customerSerde = new JsonSerde<>(Customer.class);

    @MustBeClosed
    public RedisAdapter(String redisHost) {
        pool = new JedisPool(new JedisPoolConfig(), redisHost);
    }

    @MustBeClosed
    private Jedis getJedis() {
        return pool.getResource();
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }

    @Override
    protected Customer getCustomerDetails(int customerId, int districtId, int warehouseId) {
        byte[] customer;
        try (Jedis jedis = getJedis()) {
            customer = jedis.get(getCustomerKey(customerId, districtId, warehouseId));
        }
        if (customer == null) {
            return null;
        }
        return customerSerde.deserializer().deserialize(customer);
    }

    @Override
    protected void setCustomerDetails(int customerId, int districtId, int warehouseId, Customer customer) {
        byte[] customerBinary = customerSerde.serializer().serialize(customer);
        try (Jedis jedis = getJedis()) {
            jedis.set(getCustomerKey(customerId, districtId, warehouseId), customerBinary);
        }
    }

    private byte[] getCustomerKey(int customerId, int districtId, int warehouseId) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put("CUST".getBytes(StandardCharsets.UTF_8))
            .putInt(warehouseId)
            .putInt(districtId)
            .putInt(customerId);
        return buffer.array();
    }

    // TODO

    @Override
    protected int getCustomerByName(String lastName, int districtId, int warehouseId) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    protected void setCustomerNameMapping(String lastName, int districtId, int warehouseId, int customerId) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    protected List<Integer> getRecentItems(int districtId, int warehouseId) {
        return null;
    }

    @Override
    public void addOrderItems(int districtId, int warehouseId, Collection<Integer> items) {

    }

    @Override
    public List<Integer> getStockLevels(List<Integer> items, int districtId, int warehouseId) {
        return null;
    }

    @Override
    public void updateStockLevels(int districtId, int warehouseId, Map<Integer, Integer> stockLevels) {

    }
}
