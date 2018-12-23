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
}
