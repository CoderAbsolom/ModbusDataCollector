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
     * TODO 1800个点 4线程  每个Runnable查询1个  28489ms
     * TODO 1800个点 8线程  每个Runnable查询1个  28489ms
     * TODO 1800个点 4线程  每个Runnable查询10个 15220ms
     * TODO 1800个点 8线程  每个Runnable查询10个 7925ms
     * TODO 1800个点 8线程  每个Runnable查询20个 8148ms
     * TODO 1800个点 8线程  每个Runnable查询30个 7946ms
     * TODO 1800个点 16线程 每个Runnable查询10个 4290ms
     * TODO 1800个点 16线程 每个Runnable查询20个 4264ms
     * TODO 1800个点 16线程 每个Runnable查询30个 4274ms
     */

    private static final Logger logger = LoggerFactory.getLogger(MasterDemo.class);

    private static final int POOL_SIZE = 8;

    private static final ExecutorService THREAD_POOL = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 0L,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2048),
            new ThreadFactoryBuilder().setNameFormat("modbus-pool-%d").build(),
            new ThreadPoolExecutor.AbortPolicy());

    /**
     * 生成寄存器地址列表
     * @param size
     * @param reallyAddressArray
     * @return
     */
    private static List<Integer> generateAddressList(int size, int[] reallyAddressArray) {
        Random random = new Random();
        List<Integer> resultList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            resultList.add(reallyAddressArray[random.nextInt(6)]);
        }
        return resultList;
    }

    public static void main(String[] args) throws InterruptedException {
        ModbusTcpMasterConfig config = new ModbusTcpMasterConfig.Builder("192.168.0.63").setPort(502).build();

        // 生成15个车站 * 每个站20个智能摄像头 * 每个摄像头3个点号 = 900个寄存器地址
        List<Integer> addressList = generateAddressList(15 * 40 * 3, new int[]{0, 1, 1001, 1002, 1003, 1004});

        long timer = System.currentTimeMillis();

        // 每个线程查询10个
        int slice = 30;

        for (int i = 0; i < addressList.size(); i++) {
            int index = i;
            THREAD_POOL.execute(() -> {
                try {
                    ModbusTcpMaster master = new ModbusTcpMaster(config);
                    master.connect();
                    for (int j = 0; j < slice; j++) {
                        readHoldingRegisters(master, addressList.get(index + j), 1, 1);
                    }
                    master.disconnect();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("exception {}", e.getMessage());
                }
            });
            i += slice - 1;
            logger.info("begin {} ~ {}", index, i + 1);
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

    public static void readHoldingRegisters(ModbusTcpMaster master, int address, int quantity, int unitId) throws InterruptedException, ExecutionException {
        CompletableFuture<ReadHoldingRegistersResponse> future = master.sendRequest(new ReadHoldingRegistersRequest(address, quantity), unitId);
        ReadHoldingRegistersResponse readHoldingRegistersResponse = future.get();
        if (readHoldingRegistersResponse != null) {
            ByteBuf buf = readHoldingRegistersResponse.getRegisters();
            byte[] bytes = new byte[buf.capacity()];
            buf.readBytes(bytes, 0, buf.capacity());
            StringBuilder result = new StringBuilder();
            result.append(decimalToBinary(bytes[0], 8));
            result.append(decimalToBinary(bytes[1], 8));
            logger.info(address + " ->{} ", result);
            ReferenceCountUtil.release(readHoldingRegistersResponse);
        }
    }

    public static String decimalToBinary(int num, int size) {
        if (size <(Integer.SIZE - Integer.numberOfLeadingZeros(num))) {
            throw new RuntimeException("传入size小于num二进制位数");
        }
        StringBuilder binStr = new StringBuilder();
        for(int i = size-1;i >= 0; i--){
            binStr.append(num >>> i & 1);
        }
        return binStr.toString();
    }
}