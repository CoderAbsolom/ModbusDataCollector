package com.resafety.modbus;

import com.digitalpetri.modbus.codec.Modbus;
import com.digitalpetri.modbus.master.ModbusTcpMaster;
import com.digitalpetri.modbus.master.ModbusTcpMasterConfig;
import com.digitalpetri.modbus.requests.ReadHoldingRegistersRequest;
import com.digitalpetri.modbus.responses.ReadHoldingRegistersResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author Pan Zhe
 */
public class MasterDemo {

    /*
     * TODO 记录
     * TODO 1800个点  4线程   每个线程建立1次连接、查询1个点   28489ms
     * TODO 1800个点  4线程   每个线程建立1次连接、查询10个点  15220ms
     *
     * TODO 1800个点  8线程   每个线程建立1次连接、查询1个点   28489ms
     * TODO 1800个点  8线程   每个线程建立1次连接、查询10个点  7925ms
     * TODO 1800个点  8线程   每个线程建立1次连接、查询20个点  8148ms
     * TODO 1800个点  8线程   每个线程建立1次连接、查询30个点  7946ms
     *
     * TODO 1800个点  16线程  每个线程建立1次连接、查询1个点   28494ms
     * TODO 1800个点  16线程  每个线程建立1次连接、查询10个点  4290ms
     * TODO 1800个点  16线程  每个线程建立1次连接、查询20个点  4264ms
     * TODO 1800个点  16线程  每个线程建立1次连接、查询30个点  4274ms
     */

    private static final Logger logger = LoggerFactory.getLogger(MasterDemo.class);

    private static final int POOL_SIZE = 8;

    private static final ExecutorService THREAD_POOL = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 0L,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2048),
            new ThreadFactoryBuilder().setNameFormat("modbus-pool-%d").build(),
            new ThreadPoolExecutor.AbortPolicy());

    /**
     * 生成寄存器地址列表
     *
     * @param size
     * @param reallyAddressArray
     * @return
     */
    private static List<Integer> generateAddressList(int size, int[] reallyAddressArray) {
        Random random = new Random();
        List<Integer> resultList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            resultList.add(reallyAddressArray[random.nextInt(reallyAddressArray.length)]);
        }
        return resultList;
    }

    public static void main(String[] args) throws InterruptedException {
        ModbusTcpMasterConfig config = new ModbusTcpMasterConfig
                .Builder("127.0.0.1")
                .setPort(502)
                .build();

        // 生成15个车站 * 每个站20个智能摄像头 * 每个摄像头3个点号 = 900个寄存器地址
        List<Integer> addressList = generateAddressList(15 * 20 * 3,
                new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                        1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,
                        1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020});

        long timer = System.currentTimeMillis();

        // 每个线程查询slice个
        int slice = 10;

        for (int i = 0; i < addressList.size(); i++) {
            int index = i;
            THREAD_POOL.execute(() -> {
                try {
                    ModbusTcpMaster master = new ModbusTcpMaster(config);
                    master.connect();
                    for (int j = 0; j < slice; j++) {
                        if (index + j <= addressList.size()) {
                            logger.info(index + j + "->{} ->{} ", addressList.get(index + j), readHoldingRegisters(master, addressList.get(index + j), 1, 1));
                        }
                    }
                    master.disconnect();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("exception {}", e.getMessage());
                }
            });
            i += slice - 1;
        }

        THREAD_POOL.shutdown();
        while (true) {
            if (THREAD_POOL.isTerminated()) {
                logger.info("complete {}", System.currentTimeMillis() - timer);
                break;
            }
            Thread.sleep(200);
        }
        Modbus.releaseSharedResources();
    }

    public static String readHoldingRegisters(ModbusTcpMaster master, int address, int quantity, int unitId) throws InterruptedException, ExecutionException {
        StringBuffer result = new StringBuffer();
        CompletableFuture<ReadHoldingRegistersResponse> future = master.sendRequest(new ReadHoldingRegistersRequest(address, quantity), unitId);
        ReadHoldingRegistersResponse readHoldingRegistersResponse = future.get();
        if (readHoldingRegistersResponse != null) {
            ByteBuf buf = readHoldingRegistersResponse.getRegisters();
            byte[] bytes = new byte[buf.capacity()];
            buf.readBytes(bytes, 0, buf.capacity());
            result.append(decimalToBinary(bytes[0], 8));
            result.append(decimalToBinary(bytes[1], 8));
            ReferenceCountUtil.release(readHoldingRegistersResponse);
        }
        return result.toString();
    }

    public static String decimalToBinary(int num, int size) {
        if (size < (Integer.SIZE - Integer.numberOfLeadingZeros(num))) {
            throw new RuntimeException("传入size小于num二进制位数");
        }
        StringBuilder binStr = new StringBuilder();
        for (int i = size - 1; i >= 0; i--) {
            binStr.append(num >>> i & 1);
        }
        return binStr.toString();
    }
}