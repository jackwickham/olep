package net.jackw.olep.view;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.JsonSerde;
import net.jackw.olep.view.records.Customer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

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
    public void close() {
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

    @Override
    protected int getCustomerByName(String lastName, int districtId, int warehouseId) {
        byte[] result;
        try (Jedis jedis = getJedis()) {
            result = jedis.get(getCustomerNameKey(lastName, districtId, warehouseId));
        }
        if (result == null) {
            // :(
            throw new IllegalStateException("Customer name not present in redis");
        }
        return Integer.parseInt(new String(result, UTF_8));
    }

    @Override
    protected void setCustomerNameMapping(String lastName, int districtId, int warehouseId, int customerId) {
        byte[] stringifiedCustomerId = Integer.toString(customerId).getBytes(UTF_8);
        try (Jedis jedis = getJedis()) {
            jedis.set(getCustomerNameKey(lastName, districtId, warehouseId), stringifiedCustomerId);
        }
    }

    @Override
    protected Collection<Integer> getRecentItems(int districtId, int warehouseId) {
        List<byte[]> rawItems;
        try (Jedis jedis = getJedis()) {
            rawItems = jedis.lrange(getRecentItemsKey(districtId, warehouseId), 0, 19);
        }
        // Convert the list of up to 20 orders to a stream
        return rawItems.stream()
            // Flat map the list of orders into a list of items from all of the orders
            .flatMap(items -> Streams.stream(Splitter.on(",").split(new String(items, UTF_8))))
            // Convert the item ID strings to integers
            .map(Integer::parseInt)
            // Then collect it into a set to remove duplicates
            .collect(Collectors.toSet());
    }

    @Override
    protected void addOrderItems(int districtId, int warehouseId, Collection<Integer> items) {
        byte[] stringifiedItems = Joiner.on(",").join(items).getBytes(UTF_8);
        byte[] key = getRecentItemsKey(districtId, warehouseId);
        try (Jedis jedis = getJedis()) {
            Transaction transaction = jedis.multi();
            transaction.lpush(key, stringifiedItems);
            transaction.ltrim(key, 0, 19);
            transaction.exec();
        }
    }

    @Override
    protected Collection<Integer> getStockLevels(Collection<Integer> items, int warehouseId) {
        List<byte[]> rawLevels;
        // Convert the list of desired items to a stream
        byte[][] itemKeys = items.stream()
            // Convert each item ID to a string
            .map(Object::toString)
            // and convert that string to UTF8 bytes
            .map(s -> s.getBytes(UTF_8))
            // Then convert the stream of byte[] to an array of byte[]
            .toArray(byte[][]::new);

        try (Jedis jedis = getJedis()) {
            rawLevels = jedis.hmget(getStockKey(warehouseId), itemKeys);
        }

        return rawLevels.stream()
            // Convert the byte[] to String
            .map(String::new)
            // And convert that String to an int
            .map(Integer::parseInt)
            // Then collect it as a list
            .collect(Collectors.toList());
    }

    @Override
    protected void updateStockLevels(int warehouseId, Map<Integer, Integer> stockLevels) {
        Map<byte[], byte[]> stringifiedLevels = stockLevels.entrySet().stream()
            .collect(Collectors.toMap(
                e -> Integer.toString(e.getKey()).getBytes(UTF_8),
                e -> Integer.toString(e.getValue()).getBytes(UTF_8)
            ));

        try (Jedis jedis = getJedis()) {
            jedis.hmset(getStockKey(warehouseId), stringifiedLevels);
        }
    }



    private byte[] getCustomerKey(int customerId, int districtId, int warehouseId) {
        return String.format("CUST:%d:%d:%d", warehouseId, districtId, customerId).getBytes(UTF_8);
    }

    private byte[] getCustomerNameKey(String lastName, int districtId, int warehouseId) {
        return String.format("NAME:%d:%d:%s", warehouseId, districtId, lastName).getBytes(UTF_8);
    }

    private byte[] getRecentItemsKey(int districtId, int warehouseId) {
        return String.format("ITEMS:%d:%d", warehouseId, districtId).getBytes(UTF_8);
    }

    private byte[] getStockKey(int warehouseId) {
        return String.format("STOCK:%d", warehouseId).getBytes(UTF_8);
    }
}
