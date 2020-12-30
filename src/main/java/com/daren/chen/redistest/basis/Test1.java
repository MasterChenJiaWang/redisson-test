package com.daren.chen.redistest.basis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.api.DeletedObjectListener;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBitSet;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;
import org.redisson.api.RHyperLogLog;
import org.redisson.api.RKeys;
import org.redisson.api.RLock;
import org.redisson.api.RLongAdder;
import org.redisson.api.RPatternTopic;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RTopic;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @Description:
 * @author: chendaren
 * @CreateDate: 2020/12/30 9:19
 */
public class Test1 {

    public static void main(String[] args) {
        RedissonClient redissonClient = null;
        try {
            redissonClient = init();
            //
            // testLong(redissonClient);
            //
            // testObject(redissonClient);
            //
            // testKey(redissonClient);

            //
            // testBitSet(redissonClient);

            //
            // testTopic(redissonClient);

            //
            // testPatternTopic(redissonClient);

            //
            // testBloomFilter(redissonClient);

            //
            // testHyperLogLog(redissonClient);

            //
            // testLongAdder(redissonClient);
            //
            // testRateLimiter(redissonClient);

            //
            // testReentrantLock(redissonClient);

            //
            // testFairLock(redissonClient);

            //
            testReadWriteLock(redissonClient);
        } finally {
            //
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }
    }

    private static void testReadWriteLock(RedissonClient redissonClient) {

        RReadWriteLock myReadWriteLock1 = redissonClient.getReadWriteLock("myReadWriteLock1");
        User user = new User("陈大人", 1);
        // 读锁
        RLock readLock = myReadWriteLock1.readLock();
        RLock writeLock = myReadWriteLock1.writeLock();

        new Thread(() -> {
            readLock.lock();
            try {
                Thread.sleep(1000);
                System.out.println(Thread.currentThread().getName() + " : age : " + user.getAge());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                readLock.unlock();
            }
        }, "线程1").start();

        new Thread(() -> {
            readLock.lock();
            try {
                Thread.sleep(1000);
                System.out.println(Thread.currentThread().getName() + " : age : " + user.getAge());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                readLock.unlock();
            }
        }, "线程2").start();

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            new Thread(() -> {
                writeLock.lock();
                try {
                    // Thread.sleep(1000);
                    user.setAge(3 + finalI);
                    System.out.println(Thread.currentThread().getName() + " 修改 : age : " + user.getAge());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    writeLock.unlock();
                }
            }, "线程" + 3 + i).start();
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testFairLock(RedissonClient redissonClient) {
        RLock myReentrantLock1 = redissonClient.getFairLock("myFairLock1");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                testFairLock(myReentrantLock1);
            });
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testFairLock(RLock lock) {
        boolean b = false;
        try {
            b = lock.tryLock(100, 5, TimeUnit.SECONDS);
            if (b) {
                Thread.sleep(5000);
                System.out.println("锁测试: " + Thread.currentThread().getName() + "  执行完了!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

    }

    private static void testReentrantLock(RedissonClient redissonClient) {
        RLock myReentrantLock1 = redissonClient.getLock("myReentrantLock1");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                testReentrantLock(myReentrantLock1);
            });
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testReentrantLock(RLock lock) {
        boolean b = false;
        try {
            b = lock.tryLock(100, 5, TimeUnit.SECONDS);
            if (b) {
                boolean b1 = lock.tryLock(100, 5, TimeUnit.SECONDS);
                if (b1) {
                    System.out.println("是否可重入:" + b1);
                }
                Thread.sleep(5000);
                System.out.println("锁测试: " + Thread.currentThread().getName() + "  执行完了!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

    }

    private static void testRateLimiter(RedissonClient redissonClient) {

        // RRateLimiter myRateLimiter = redissonClient.getRateLimiter("myRateLimiter3");
        //
        // boolean b = myRateLimiter.trySetRate(RateType.PER_CLIENT, 5, 2, RateIntervalUnit.MINUTES);
        // System.out.println(b);
        // // CountDownLatch countDownLatch = new CountDownLatch(2);
        //
        // ExecutorService executorService = Executors.newFixedThreadPool(10);
        // for (int i = 0; i < 10; i++) {
        // executorService.submit(() -> {
        // try {
        // myRateLimiter.acquire();
        // System.out
        // .println("线程 " + Thread.currentThread().getName() + "进入数据区：" + System.currentTimeMillis());
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // });
        // }
        RRateLimiter myRateLimiter4 = redissonClient.getRateLimiter("myRateLimiter5");
        myRateLimiter4.trySetRate(RateType.PER_CLIENT, 1, 1, RateIntervalUnit.SECONDS);
        int n = 0;
        while (n < 10) {
            if (myRateLimiter4.tryAcquire()) {
                System.out.println("数据: " + System.currentTimeMillis());
                n++;
            }
        }
    }

    private static void testLongAdder(RedissonClient redissonClient) {
        RLongAdder atomicLong = redissonClient.getLongAdder("myLongAdder");
        atomicLong.add(12);
        atomicLong.increment();
        atomicLong.decrement();
        long sum = atomicLong.sum();
        System.out.println(sum);
        // 会自动销毁
    }

    private static void testHyperLogLog(RedissonClient redissonClient) {
        RHyperLogLog<Integer> hyperLogLog = redissonClient.getHyperLogLog("myHyperLogLog");
        hyperLogLog.add(1);
        hyperLogLog.add(1);
        hyperLogLog.add(1);
        hyperLogLog.add(2);
        //
        System.out.println(hyperLogLog.count());
        //
        RHyperLogLog<User> hyperLogLog2 = redissonClient.getHyperLogLog("myHyperLogLog2");
        hyperLogLog2.add(new User("0", 1));
        hyperLogLog2.add(new User("0", 1));
        hyperLogLog2.add(new User("1", 1));
        hyperLogLog2.add(new User("2", 2));
        //
        System.out.println(hyperLogLog2.count());
    }

    private static void testBloomFilter(RedissonClient redissonClient) {
        RBloomFilter<User> bloomFilter = redissonClient.getBloomFilter("myBloomFilter");

        // bloomFilter.tryInit(100000000L, 0.03);
        //
        //
        // for (int i = 1; i <= 10000; i++) {
        // bloomFilter.add(new User("陈大人" + i, i));
        // }
        //
        // boolean b = bloomFilter.contains(new User("陈大人66", 66));
        // System.out.println(b);
        // // 预期插入
        // long expectedInsertions = bloomFilter.getExpectedInsertions();
        // System.out.println(expectedInsertions);
        // // 错误概率
        // double falseProbability = bloomFilter.getFalseProbability();
        // System.out.println(falseProbability);
        //
        // int hashIterations = bloomFilter.getHashIterations();
        // System.out.println(hashIterations);

        boolean contains = bloomFilter.contains(new User("陈大人", 3));
        System.out.println(contains);
        bloomFilter.delete();
    }

    private static void testPatternTopic(RedissonClient redissonClient) {
        new Thread(() -> {
            RPatternTopic myPatternTopic = redissonClient.getPatternTopic("topic*");
            myPatternTopic.addListener(User.class, (charSequence, charSequence2, user) -> {
                System.out.println("主题1名称:" + charSequence.toString());
                System.out.println("主题2名称:" + charSequence2.toString());
                System.out.println("收到消息:" + user.toString());
            });
        }, "线程1").start();

        new Thread(() -> {
            RTopic topic2 = redissonClient.getTopic("topic1");
            topic2.publish(new User("陈大人消息测试发布1", 11));
        }, "线程2").start();

        new Thread(() -> {
            RTopic topic2 = redissonClient.getTopic("topic3");
            topic2.publishAsync(new User("陈大人消息测试发布3", 33));
        }, "线程3").start();

        new Thread(() -> {
            RTopic topic2 = redissonClient.getTopic("topic4");
            topic2.publishAsync(new User("陈大人消息测试发布4", 44));
        }, "线程4").start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testTopic(RedissonClient redissonClient) {
        new Thread(() -> {
            RTopic topic = redissonClient.getTopic("myTopic");
            topic.addListener(User.class, (charSequence, user) -> {
                System.out.println("主题名称:" + charSequence.toString());
                System.out.println("收到消息:" + user.toString());
            });
        }, "线程1").start();

        new Thread(() -> {
            RTopic topic2 = redissonClient.getTopic("myTopic");
            topic2.publish(new User("陈大人消息测试发布1", 22));
        }, "线程2").start();

        new Thread(() -> {
            RTopic topic2 = redissonClient.getTopic("myTopic");
            topic2.publishAsync(new User("陈大人消息测试发布2", 33));
        }, "线程3").start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testBitSet(RedissonClient redissonClient) {
        RBitSet simpleBitset = redissonClient.getBitSet("simpleBitset");
        // simpleBitset.set(0, true);
        // simpleBitset.set(1812, false);
        // boolean b = simpleBitset.get(0);
        // System.out.println(b);
        // boolean b1 = simpleBitset.get(1);
        // System.out.println(b1);
        // simpleBitset.clear(0);
        // simpleBitset.andAsync("e");
        // simpleBitset.xor("anotherBitset");

        Random random = new Random();

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10000000; i++) {
            int randomResult = random.nextInt(100000000);
            list.add(randomResult);
        }
        System.out.println("产生的随机数有");
        for (Integer integer : list) {
            System.out.println(integer);
        }
        BitSet bitSet = new BitSet(100000000);
        for (int i = 0; i < 10000000; i++) {
            bitSet.set(list.get(i));
        }

        System.out.println("0~1亿不在上述随机数中有" + bitSet.cardinality());
        for (int i = 0; i < 100000000; i++) {
            if (!bitSet.get(i)) {
                System.out.println(i);
            }
        }
    }

    private static void testKey(RedissonClient redissonClient) {

        RKeys keys = redissonClient.getKeys();

        // Iterable<String> keys1 = keys.getKeys();
        // for (String s : keys1) {
        // System.out.println(s);
        // }

        Iterable<String> keysByPattern = keys.getKeysByPattern("cup-auth-101:userInfo:*");
        for (String s : keysByPattern) {
            System.out.println(s);
        }

        long l = keys.deleteByPattern("cup-auth-101:userInfo:*");
        System.out.println(l);
    }

    private static void testObject(RedissonClient redissonClient) {
        RBucket<User> userRBucket = redissonClient.getBucket("myObject");
        RBucket<User> userRBucket2 = redissonClient.getBucket("myObject2");

        User user = userRBucket.get();
        System.out.println(user == null ? "" : user.toString());

        userRBucket.addListener(new DeletedObjectListener() {
            @Override
            public void onDeleted(String s) {
                System.out.println("删除 监听 :  " + s);
            }
        });
        //
        userRBucket.set(new User("陈大人", 1));
        userRBucket2.set(new User("陈大人22", 22));
        //
        User user1 = userRBucket.get();
        System.out.println(user1.toString());
        //
        User andDelete = userRBucket.getAndDelete();
        System.out.println(andDelete.toString());
        //
        boolean exists = userRBucket.isExists();
        System.out.println("isExists  :" + exists);

        //
        userRBucket.trySet(new User("陈大人2", 2));
        userRBucket.trySet(new User("陈大人3", 3));

        boolean b = userRBucket.compareAndSet(new User("陈大人2", 2), new User("陈大人4", 4));
        System.out.println(b);
        //
        User user2 = userRBucket.get();
        System.out.println(user2);

        //
        RBuckets buckets = redissonClient.getBuckets();

        Map<String, Object> map = buckets.get("myObject", "myObject2");
        map.forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });

    }

    /**
     *
     * @param redissonClient
     */
    private static void testLong(RedissonClient redissonClient) {
        RAtomicLong myLong = redissonClient.getAtomicLong("myLong");
        System.out.println(myLong.get());

        myLong.set(2);
        System.out.println(myLong.get());

        long l = myLong.incrementAndGet();
        System.out.println(l);
        System.out.println(myLong.get());
        System.out.println("..........");
        //
        long l1 = myLong.decrementAndGet();
        System.out.println(l1);
        System.out.println(myLong.get());
        System.out.println("..........");
        //
        long l2 = myLong.addAndGet(5);
        System.out.println(l2 + " 值: " + myLong.get());
        //

        long andAdd = myLong.getAndAdd(10);
        System.out.println(andAdd + " 值: " + myLong.get());
        //

    }

    private static RedissonClient init() {
        Config config = new Config();
        // config.setCodec(new JsonJacksonCodec());
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setDatabase(4);
        return Redisson.create(config);
    }

    static class User implements Serializable {

        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
