package test;

import redis.clients.jedis.Jedis;

public class TestReplication {
    public static void main(String[] args) {
        Jedis jedis_M = new Jedis("master", 6379);
        Jedis jedis_S1 = new Jedis("slave1", 6379);
        Jedis jedis_S2 = new Jedis("slave2", 6379);
        Jedis jedis_S3 = new Jedis("slave3", 6379);

        jedis_S1.slaveof("master", 6379);
        jedis_S2.slaveof("master", 6379);
        jedis_S3.slaveof("master", 6379);

        jedis_M.set("class", "1122V2");

        String result = jedis_S1.get("class");//可能有延迟，需再次启动才能使用
        System.out.println(result);
    }
}
