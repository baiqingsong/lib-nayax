package com.dawn.nayax;

import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.text.TextUtils;

import com.dawn.serial.LSerialUtil;

import java.lang.ref.WeakReference;

/**
 * Nayax刷卡设备管理器
 * <p>
 * 提供串口连接、设备初始化、支付流程管理、心跳保活及自动重连功能。
 * 使用单例模式，通过 {@link #getInstance()} 获取实例。
 * </p>
 * <p>使用流程：</p>
 * <ol>
 *   <li>{@link #setCallback(NayaxCallback)} 设置回调</li>
 *   <li>{@link #connect(int)} 连接串口（自动初始化设备）</li>
 *   <li>等待 {@link NayaxCallback#onDeviceReady} 回调</li>
 *   <li>{@link #startPayment(float)} 发起收款</li>
 *   <li>等待 {@link NayaxCallback#onPaymentCompleted(float)} 回调</li>
 *   <li>{@link #reportSaleResult()} 上报售卖结果</li>
 *   <li>{@link #disconnect()} 断开连接</li>
 * </ol>
 *
 * <p>稳定性特性：</p>
 * <ul>
 *   <li>心跳保活：空闲时定期发送状态查询，检测静默断连</li>
 *   <li>初始化超时：连接后设备未在指定时间内就绪则自动重连</li>
 *   <li>操作超时重试：完成/取消/售卖操作自动重试</li>
 *   <li>数据缓冲：处理串口数据分片问题</li>
 *   <li>自动重连：断连后指数退避重连</li>
 * </ul>
 */
public class NayaxManager {

    private static final String TAG = "NayaxManager";

    // ==================== 错误码 ====================
    /** 串口打开失败 */
    public static final int ERROR_PORT_OPEN = 1;
    /** 发送失败 */
    public static final int ERROR_SEND = 2;
    /** 接收异常 */
    public static final int ERROR_RECEIVE = 3;
    /** 收款超时 */
    public static final int ERROR_PAYMENT_TIMEOUT = 4;
    /** 响应数据无效 */
    public static final int ERROR_INVALID_RESPONSE = 5;
    /** 重连次数耗尽 */
    public static final int ERROR_RECONNECT_EXHAUSTED = 6;
    /** 设备未就绪 */
    public static final int ERROR_NOT_READY = 7;
    /** 初始化超时 */
    public static final int ERROR_INIT_TIMEOUT = 8;
    /** 操作响应超时 */
    public static final int ERROR_OPERATION_TIMEOUT = 9;

    // ==================== Handler消息类型 ====================
    private static final int MSG_QUERY_STATUS = 0x01;
    private static final int MSG_QUERY_MIN_AMOUNT = 0x02;
    private static final int MSG_POLL_PAYMENT = 0x03;
    private static final int MSG_PAYMENT_TIMEOUT = 0x04;
    private static final int MSG_COMPLETE_PAYMENT = 0x05;
    private static final int MSG_RECONNECT = 0x06;
    private static final int MSG_HEARTBEAT = 0x07;
    private static final int MSG_INIT_TIMEOUT = 0x08;
    private static final int MSG_OPERATION_TIMEOUT = 0x09;
    private static final int MSG_HEARTBEAT_TIMEOUT = 0x0A;

    // ==================== 时间常量(ms) ====================
    private static final long INIT_QUERY_DELAY_MS = 3000L;
    private static final long STATUS_RETRY_MS = 5000L;
    private static final long MIN_AMOUNT_RETRY_MS = 5000L;
    private static final long PAYMENT_FIRST_POLL_MS = 3000L;
    private static final long PAYMENT_POLL_MS = 2000L;
    private static final long PAYMENT_TIMEOUT_MS = 90_000L;
    private static final long COMPLETE_DELAY_MS = 3000L;
    private static final long RECONNECT_BASE_DELAY_MS = 3000L;
    private static final long HEARTBEAT_INTERVAL_MS = 30_000L;
    private static final long HEARTBEAT_TIMEOUT_MS = 5000L;
    private static final long INIT_TIMEOUT_MS = 30_000L;
    private static final long OPERATION_TIMEOUT_MS = 10_000L;

    // ==================== 串口配置 ====================
    private static final int BAUD_RATE = 9600;
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private static final int MAX_OPERATION_RETRIES = 3;

    // ==================== 预期响应常量 ====================
    private static final String RESP_START_PAYMENT_OK = "E11020040003DC69";
    private static final String RESP_COMPLETE_OK = "E106100100010B6A";
    private static final String RESP_CANCEL_OK = "E10610020001FB6A";
    private static final String RESP_SALE_OK = "E10610030001AAAA";
    private static final int RESPONSE_LENGTH_STANDARD = 18;

    // ==================== 数据缓冲 ====================
    private static final int MAX_BUFFER_SIZE = 128;

    // ==================== 状态机 ====================
    private enum State {
        IDLE,
        QUERYING_STATUS,
        QUERYING_MIN_AMOUNT,
        STARTING_PAYMENT,
        POLLING_PAYMENT,
        COMPLETING_PAYMENT,
        CANCELLING_PAYMENT,
        REPORTING_SALE
    }

    // ==================== 单例 ====================
    private static volatile NayaxManager instance;

    // ==================== 核心字段 ====================
    private final Handler handler;
    private LSerialUtil serialUtil;
    private NayaxCallback callback;

    // ==================== 状态字段 ====================
    private State currentState = State.IDLE;
    private volatile boolean paying = false;
    private volatile boolean deviceReady = false;
    private volatile boolean heartbeatPending = false;
    private volatile boolean manualDisconnect = false;

    // ==================== 连接字段 ====================
    private int serialPort;
    private int reconnectCount = 0;
    private int operationRetryCount = 0;

    // ==================== 数据缓冲 ====================
    private final StringBuilder dataBuffer = new StringBuilder();

    // ==================== 设备与支付信息 ====================
    private float minAmount;
    private float requestedAmount;
    private float receivedAmount;
    private String deviceVersion;
    private String deviceType;
    private String currencyCode;

    // ==================== 构造 ====================

    private NayaxManager() {
        handler = new SafeHandler(this);
    }

    public static NayaxManager getInstance() {
        if (instance == null) {
            synchronized (NayaxManager.class) {
                if (instance == null) {
                    instance = new NayaxManager();
                }
            }
        }
        return instance;
    }

    // ==================== 公开API ====================

    /**
     * 设置回调监听
     */
    public void setCallback(NayaxCallback callback) {
        this.callback = callback;
    }

    /**
     * 连接串口并初始化设备
     * <p>连接成功后自动查询设备状态和最小面额，
     * 初始化完成后通过 {@link NayaxCallback#onDeviceReady} 回调通知。</p>
     * <p>若设备在 {@value INIT_TIMEOUT_MS}ms 内未就绪，将自动重连。</p>
     *
     * @param port 串口号
     */
    public void connect(int port) {
        if (port < 0) {
            notifyError(ERROR_PORT_OPEN, "无效的串口号: " + port);
            return;
        }
        this.serialPort = port;
        this.reconnectCount = 0;
        this.manualDisconnect = false;
        openSerialPort();
    }

    /**
     * 断开串口连接
     */
    public void disconnect() {
        manualDisconnect = true;
        removeAllMessages();
        paying = false;
        deviceReady = false;
        heartbeatPending = false;
        currentState = State.IDLE;
        dataBuffer.setLength(0);
        closeSerialPort();
    }

    /**
     * 释放所有资源并销毁单例
     */
    public void release() {
        disconnect();
        handler.removeCallbacksAndMessages(null);
        callback = null;
        synchronized (NayaxManager.class) {
            instance = null;
        }
    }

    /**
     * 发起收款
     *
     * @param amount 收款金额（必须大于0）
     */
    public void startPayment(float amount) {
        if (paying) {
            PaymentLog.w(TAG, "收款进行中，忽略重复请求");
            return;
        }
        if (!isConnected()) {
            notifyError(ERROR_NOT_READY, "串口未连接");
            return;
        }
        if (!deviceReady || minAmount <= 0) {
            notifyError(ERROR_NOT_READY, "设备未就绪，请等待初始化完成");
            return;
        }
        if (amount <= 0) {
            notifyError(ERROR_SEND, "收款金额必须大于0");
            return;
        }

        stopHeartbeat();
        paying = true;
        requestedAmount = amount;
        receivedAmount = 0;
        currentState = State.STARTING_PAYMENT;
        PaymentLog.i(TAG, "发起收款: " + amount);
        sendCommand(NayaxCommand.getStartMoney(amount, minAmount));
    }

    /**
     * 取消收款
     */
    public void cancelPayment() {
        if (!paying) {
            PaymentLog.w(TAG, "当前无收款进行中");
            return;
        }
        handler.removeMessages(MSG_POLL_PAYMENT);
        handler.removeMessages(MSG_PAYMENT_TIMEOUT);
        handler.removeMessages(MSG_COMPLETE_PAYMENT);
        currentState = State.CANCELLING_PAYMENT;
        operationRetryCount = 0;
        sendCommand(NayaxCommand.getCancelMoney());
        startOperationTimeout();
    }

    /**
     * 手动确认收款完成
     * <p>通常由系统自动完成，仅在需要手动干预时调用</p>
     */
    public void confirmPayment() {
        currentState = State.COMPLETING_PAYMENT;
        operationRetryCount = 0;
        sendCommand(NayaxCommand.getCompleteMoney());
        startOperationTimeout();
    }

    /**
     * 上报售卖结果（成功）
     */
    public void reportSaleResult() {
        currentState = State.REPORTING_SALE;
        operationRetryCount = 0;
        sendCommand(NayaxCommand.setSaleResult());
        startOperationTimeout();
    }

    /**
     * 手动重新查询设备信息
     */
    public void queryDeviceInfo() {
        stopHeartbeat();
        handler.removeMessages(MSG_QUERY_STATUS);
        handler.removeMessages(MSG_QUERY_MIN_AMOUNT);
        deviceReady = false;
        cycleQueryStatus();
    }

    /**
     * 串口是否已连接
     */
    public boolean isConnected() {
        return serialUtil != null && serialUtil.isConnected();
    }

    /**
     * 设备是否已初始化就绪
     */
    public boolean isReady() {
        return deviceReady;
    }

    /**
     * 是否正在收款
     */
    public boolean isPaying() {
        return paying;
    }

    /**
     * 获取当前串口号
     */
    public int getCurrentPort() {
        return serialPort;
    }

    /**
     * 获取设备支持的最小面额
     */
    public float getMinAmount() {
        return minAmount;
    }

    /**
     * 获取控制板版本
     */
    public String getDeviceVersion() {
        return deviceVersion;
    }

    /**
     * 获取外设类型
     */
    public String getDeviceType() {
        return deviceType;
    }

    /**
     * 获取货币代码
     */
    public String getCurrencyCode() {
        return currencyCode;
    }

    // ==================== 串口管理 ====================

    private void openSerialPort() {
        closeSerialPort();
        dataBuffer.setLength(0);

        serialUtil = new LSerialUtil(serialPort, BAUD_RATE, LSerialUtil.SerialType.TYPE_HEX,
                new LSerialUtil.OnSerialListener() {
                    @Override
                    public void onOpenError(String portPath, Exception e) {
                        PaymentLog.e(TAG, "串口打开失败: " + portPath, e);
                    }

                    @Override
                    public void onReceiveError(Exception e) {
                        PaymentLog.e(TAG, "串口接收异常", e);
                        notifyError(ERROR_RECEIVE, "串口接收异常: " + e.getMessage());
                        handler.post(new Runnable() {
                            @Override
                            public void run() {
                                if (serialUtil != null && !serialUtil.isConnected()) {
                                    onRuntimeDisconnect();
                                }
                            }
                        });
                    }

                    @Override
                    public void onSendError(Exception e) {
                        PaymentLog.e(TAG, "串口发送异常", e);
                        notifyError(ERROR_SEND, "串口发送异常: " + e.getMessage());
                    }

                    @Override
                    public void onDataReceived(String data) {
                        if (!TextUtils.isEmpty(data)) {
                            PaymentLog.d(TAG, "收到原始数据: " + data);
                            handleSerialData(data);
                        }
                    }
                });

        if (!serialUtil.isConnected()) {
            serialUtil = null;
            handlePortOpenFailed();
            return;
        }

        reconnectCount = 0;
        notifyConnectionChanged(true);
        // 延迟后开始初始化查询
        handler.sendEmptyMessageDelayed(MSG_QUERY_STATUS, INIT_QUERY_DELAY_MS);
        // 设置初始化超时
        handler.sendEmptyMessageDelayed(MSG_INIT_TIMEOUT, INIT_TIMEOUT_MS);
    }

    private void closeSerialPort() {
        boolean wasConnected = isConnected();
        if (serialUtil != null) {
            try {
                serialUtil.disconnect();
            } catch (Exception e) {
                PaymentLog.w(TAG, "关闭串口异常", e);
            }
            serialUtil = null;
        }
        if (wasConnected) {
            notifyConnectionChanged(false);
        }
    }

    private void handlePortOpenFailed() {
        notifyError(ERROR_PORT_OPEN, "串口打开失败，端口: " + serialPort);
        attemptReconnect();
    }

    private void attemptReconnect() {
        if (manualDisconnect) {
            PaymentLog.d(TAG, "手动断连，不自动重连");
            return;
        }
        reconnectCount++;
        if (reconnectCount > MAX_RECONNECT_ATTEMPTS) {
            notifyError(ERROR_RECONNECT_EXHAUSTED,
                    "重连失败，已达最大重试次数: " + MAX_RECONNECT_ATTEMPTS);
            return;
        }
        long delay = RECONNECT_BASE_DELAY_MS * reconnectCount;
        PaymentLog.i(TAG, "准备第 " + reconnectCount + " 次重连，延迟 " + delay + "ms");
        handler.sendEmptyMessageDelayed(MSG_RECONNECT, delay);
    }

    /**
     * 运行中串口断开处理
     */
    private void onRuntimeDisconnect() {
        PaymentLog.w(TAG, "运行中串口断开");
        removeAllMessages();
        if (paying) {
            paying = false;
            notifyError(ERROR_RECEIVE, "收款过程中串口断开");
        }
        deviceReady = false;
        heartbeatPending = false;
        currentState = State.IDLE;
        dataBuffer.setLength(0);
        closeSerialPort();
        reconnectCount = 0;
        attemptReconnect();
    }

    // ==================== 心跳保活 ====================

    private void startHeartbeat() {
        if (paying || !isConnected()) return;
        handler.removeMessages(MSG_HEARTBEAT);
        handler.sendEmptyMessageDelayed(MSG_HEARTBEAT, HEARTBEAT_INTERVAL_MS);
    }

    private void stopHeartbeat() {
        handler.removeMessages(MSG_HEARTBEAT);
        handler.removeMessages(MSG_HEARTBEAT_TIMEOUT);
        heartbeatPending = false;
    }

    private void performHeartbeat() {
        if (paying || !isConnected()) return;
        heartbeatPending = true;
        currentState = State.QUERYING_STATUS;
        sendCommand(NayaxCommand.getMDBCommand());
        handler.sendEmptyMessageDelayed(MSG_HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT_MS);
    }

    // ==================== 操作超时 ====================

    private void startOperationTimeout() {
        handler.removeMessages(MSG_OPERATION_TIMEOUT);
        handler.sendEmptyMessageDelayed(MSG_OPERATION_TIMEOUT, OPERATION_TIMEOUT_MS);
    }

    private void handleOperationTimeout() {
        operationRetryCount++;
        if (operationRetryCount > MAX_OPERATION_RETRIES) {
            PaymentLog.e(TAG, "操作超时，已达最大重试次数");
            notifyError(ERROR_OPERATION_TIMEOUT, "操作响应超时，状态: " + currentState);
            if (paying) {
                paying = false;
            }
            currentState = State.IDLE;
            startHeartbeat();
            return;
        }

        PaymentLog.w(TAG, "操作超时，第 " + operationRetryCount + " 次重试，状态: " + currentState);
        switch (currentState) {
            case COMPLETING_PAYMENT:
                sendCommand(NayaxCommand.getCompleteMoney());
                startOperationTimeout();
                break;
            case CANCELLING_PAYMENT:
                sendCommand(NayaxCommand.getCancelMoney());
                startOperationTimeout();
                break;
            case REPORTING_SALE:
                sendCommand(NayaxCommand.setSaleResult());
                startOperationTimeout();
                break;
            default:
                currentState = State.IDLE;
                startHeartbeat();
                break;
        }
    }

    // ==================== 轮询方法 ====================

    private void cycleQueryStatus() {
        handler.removeMessages(MSG_QUERY_STATUS);
        currentState = State.QUERYING_STATUS;
        sendCommand(NayaxCommand.getMDBCommand());
        handler.sendEmptyMessageDelayed(MSG_QUERY_STATUS, STATUS_RETRY_MS);
    }

    private void cycleQueryMinAmount() {
        handler.removeMessages(MSG_QUERY_MIN_AMOUNT);
        currentState = State.QUERYING_MIN_AMOUNT;
        sendCommand(NayaxCommand.getMinMoney());
        handler.sendEmptyMessageDelayed(MSG_QUERY_MIN_AMOUNT, MIN_AMOUNT_RETRY_MS);
    }

    private void cyclePollPayment() {
        handler.removeMessages(MSG_POLL_PAYMENT);
        currentState = State.POLLING_PAYMENT;
        sendCommand(NayaxCommand.getMoney());
        handler.sendEmptyMessageDelayed(MSG_POLL_PAYMENT, PAYMENT_POLL_MS);
    }

    // ==================== 数据缓冲处理 ====================

    private void handleSerialData(String data) {
        if (TextUtils.isEmpty(data)) return;
        dataBuffer.append(data.trim().toUpperCase());
        processBuffer();
    }

    private void processBuffer() {
        while (dataBuffer.length() >= 14) {
            // 查找帧起始标记 E1
            int startIdx = dataBuffer.indexOf("E1");
            if (startIdx < 0) {
                dataBuffer.setLength(0);
                return;
            }
            if (startIdx > 0) {
                dataBuffer.delete(0, startIdx);
            }

            String buffered = dataBuffer.toString();
            boolean foundFrame = false;

            // 尝试不同帧长度进行CRC校验（最小14字符，最大64字符）
            for (int len = 14; len <= buffered.length() && len <= 64; len += 2) {
                String candidate = buffered.substring(0, len);
                if (NayaxCommand.validateResponse(candidate)) {
                    dataBuffer.delete(0, len);
                    PaymentLog.d(TAG, "解析完整帧: " + candidate);
                    processValidFrame(candidate);
                    foundFrame = true;
                    break;
                }
            }

            if (!foundFrame) {
                if (dataBuffer.length() > MAX_BUFFER_SIZE) {
                    PaymentLog.w(TAG, "缓冲区溢出，清除: " + dataBuffer);
                    dataBuffer.setLength(0);
                }
                return;
            }
        }
    }

    private void processValidFrame(String data) {
        switch (currentState) {
            case QUERYING_STATUS:
                handleStatusResponse(data);
                break;
            case QUERYING_MIN_AMOUNT:
                handleMinAmountResponse(data);
                break;
            case STARTING_PAYMENT:
                handleStartPaymentResponse(data);
                break;
            case POLLING_PAYMENT:
                handlePaymentResponse(data);
                break;
            case COMPLETING_PAYMENT:
                handleCompleteResponse(data);
                break;
            case CANCELLING_PAYMENT:
                handleCancelResponse(data);
                break;
            case REPORTING_SALE:
                handleSaleResponse(data);
                break;
            default:
                PaymentLog.d(TAG, "状态 " + currentState + " 下收到未处理数据: " + data);
                break;
        }
    }

    // ==================== 响应处理 ====================

    private void handleStatusResponse(String data) {
        if (data.length() != RESPONSE_LENGTH_STANDARD) return;
        handler.removeMessages(MSG_QUERY_STATUS);
        deviceVersion = data.substring(6, 8);
        deviceType = data.substring(8, 10);
        currencyCode = data.substring(10, 14);
        PaymentLog.i(TAG, "设备状态: version=" + deviceVersion
                + " type=" + deviceType + " currency=" + currencyCode);

        if (heartbeatPending) {
            // 心跳响应 — 设备仍然在线
            heartbeatPending = false;
            handler.removeMessages(MSG_HEARTBEAT_TIMEOUT);
            currentState = State.IDLE;
            startHeartbeat();
        } else {
            // 正常初始化流程，继续查询最小面额
            cycleQueryMinAmount();
        }
    }

    private void handleMinAmountResponse(String data) {
        if (data.length() != RESPONSE_LENGTH_STANDARD) return;
        try {
            handler.removeMessages(MSG_QUERY_MIN_AMOUNT);
            handler.removeMessages(MSG_INIT_TIMEOUT);
            int base = Integer.parseInt(data.substring(6, 10), 16);
            int decimals = Integer.parseInt(data.substring(10, 14), 16);
            minAmount = (float) (base / Math.pow(10, decimals));
            deviceReady = true;
            currentState = State.IDLE;
            PaymentLog.i(TAG, "设备就绪: minAmount=" + minAmount);
            NayaxCallback cb = this.callback;
            if (cb != null) {
                cb.onDeviceReady(deviceVersion, deviceType, currencyCode, minAmount);
            }
            // 设备就绪，启动心跳保活
            startHeartbeat();
        } catch (NumberFormatException e) {
            PaymentLog.e(TAG, "最小面额解析失败", e);
            notifyError(ERROR_INVALID_RESPONSE, "最小面额响应解析失败");
        }
    }

    private void handleStartPaymentResponse(String data) {
        if (!RESP_START_PAYMENT_OK.equalsIgnoreCase(data)) return;
        PaymentLog.i(TAG, "收款已发起");
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onPaymentStarted();
        }
        handler.sendEmptyMessageDelayed(MSG_POLL_PAYMENT, PAYMENT_FIRST_POLL_MS);
        handler.sendEmptyMessageDelayed(MSG_PAYMENT_TIMEOUT, PAYMENT_TIMEOUT_MS);
    }

    private void handlePaymentResponse(String data) {
        if (data.length() != RESPONSE_LENGTH_STANDARD) return;
        try {
            String payType = data.substring(6, 8);
            int multiple = Integer.parseInt(data.substring(8, 14), 16);
            float actualAmount = multiple * minAmount;

            PaymentLog.i(TAG, "支付检测: multiple=" + multiple + " actual=" + actualAmount
                    + " requested=" + requestedAmount);

            if (Math.round(requestedAmount * 100) <= Math.round(actualAmount * 100)) {
                // 金额已满足，停止轮询，准备完成收款
                receivedAmount = requestedAmount;
                handler.removeMessages(MSG_POLL_PAYMENT);
                handler.removeMessages(MSG_PAYMENT_TIMEOUT);

                NayaxCallback cb = this.callback;
                if (cb != null) {
                    cb.onPaymentReceived(payType, actualAmount);
                }
                handler.sendEmptyMessageDelayed(MSG_COMPLETE_PAYMENT, COMPLETE_DELAY_MS);
            } else if (multiple > 0) {
                // 部分付款，通知进度
                NayaxCallback cb = this.callback;
                if (cb != null) {
                    cb.onPartialPayment(actualAmount, requestedAmount);
                }
            }
        } catch (NumberFormatException e) {
            PaymentLog.e(TAG, "收款金额解析失败", e);
            notifyError(ERROR_INVALID_RESPONSE, "收款金额响应解析失败");
        }
    }

    private void handleCompleteResponse(String data) {
        if (!RESP_COMPLETE_OK.equalsIgnoreCase(data)) return;
        handler.removeMessages(MSG_OPERATION_TIMEOUT);
        paying = false;
        PaymentLog.i(TAG, "收款完成: " + receivedAmount);
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onPaymentCompleted(receivedAmount);
        }
        currentState = State.IDLE;
        startHeartbeat();
    }

    private void handleCancelResponse(String data) {
        if (!RESP_CANCEL_OK.equalsIgnoreCase(data)) return;
        handler.removeMessages(MSG_OPERATION_TIMEOUT);
        paying = false;
        handler.removeMessages(MSG_POLL_PAYMENT);
        handler.removeMessages(MSG_PAYMENT_TIMEOUT);
        handler.removeMessages(MSG_COMPLETE_PAYMENT);
        PaymentLog.i(TAG, "收款已取消");
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onPaymentCancelled();
        }
        currentState = State.IDLE;
        startHeartbeat();
    }

    private void handleSaleResponse(String data) {
        if (!RESP_SALE_OK.equalsIgnoreCase(data)) return;
        handler.removeMessages(MSG_OPERATION_TIMEOUT);
        PaymentLog.i(TAG, "售卖结果已确认");
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onSaleResult(true);
        }
        currentState = State.IDLE;
        startHeartbeat();
    }

    // ==================== 内部工具 ====================

    private void sendCommand(String command) {
        if (TextUtils.isEmpty(command)) {
            PaymentLog.w(TAG, "指令为空，跳过发送");
            return;
        }
        if (serialUtil == null || !serialUtil.isConnected()) {
            PaymentLog.w(TAG, "串口未连接，无法发送: " + command);
            return;
        }
        PaymentLog.d(TAG, "发送指令: " + command);
        serialUtil.sendHex(command);
    }

    private void removeAllMessages() {
        handler.removeMessages(MSG_QUERY_STATUS);
        handler.removeMessages(MSG_QUERY_MIN_AMOUNT);
        handler.removeMessages(MSG_POLL_PAYMENT);
        handler.removeMessages(MSG_PAYMENT_TIMEOUT);
        handler.removeMessages(MSG_COMPLETE_PAYMENT);
        handler.removeMessages(MSG_RECONNECT);
        handler.removeMessages(MSG_HEARTBEAT);
        handler.removeMessages(MSG_INIT_TIMEOUT);
        handler.removeMessages(MSG_OPERATION_TIMEOUT);
        handler.removeMessages(MSG_HEARTBEAT_TIMEOUT);
    }

    private void notifyError(int code, String message) {
        PaymentLog.e(TAG, "错误[" + code + "]: " + message);
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onError(code, message);
        }
    }

    private void notifyConnectionChanged(boolean connected) {
        NayaxCallback cb = this.callback;
        if (cb != null) {
            cb.onConnectionChanged(connected);
        }
    }

    // ==================== Handler消息处理 ====================

    private void handleMsg(Message msg) {
        switch (msg.what) {
            case MSG_QUERY_STATUS:
                cycleQueryStatus();
                break;
            case MSG_QUERY_MIN_AMOUNT:
                cycleQueryMinAmount();
                break;
            case MSG_POLL_PAYMENT:
                cyclePollPayment();
                break;
            case MSG_PAYMENT_TIMEOUT:
                paying = false;
                handler.removeMessages(MSG_POLL_PAYMENT);
                PaymentLog.w(TAG, "收款超时");
                notifyError(ERROR_PAYMENT_TIMEOUT, "收款超时");
                currentState = State.IDLE;
                startHeartbeat();
                break;
            case MSG_COMPLETE_PAYMENT:
                // 注意：paying状态在收到设备确认响应后才清除
                currentState = State.COMPLETING_PAYMENT;
                operationRetryCount = 0;
                sendCommand(NayaxCommand.getCompleteMoney());
                startOperationTimeout();
                break;
            case MSG_RECONNECT:
                openSerialPort();
                break;
            case MSG_HEARTBEAT:
                performHeartbeat();
                break;
            case MSG_HEARTBEAT_TIMEOUT:
                if (heartbeatPending) {
                    heartbeatPending = false;
                    PaymentLog.w(TAG, "心跳超时，设备可能已断连");
                    onRuntimeDisconnect();
                }
                break;
            case MSG_INIT_TIMEOUT:
                if (!deviceReady) {
                    PaymentLog.w(TAG, "设备初始化超时");
                    notifyError(ERROR_INIT_TIMEOUT, "设备初始化超时，正在重连");
                    removeAllMessages();
                    closeSerialPort();
                    reconnectCount = 0;
                    attemptReconnect();
                }
                break;
            case MSG_OPERATION_TIMEOUT:
                handleOperationTimeout();
                break;
        }
    }

    /**
     * 静态Handler，避免内存泄漏
     */
    private static class SafeHandler extends Handler {
        private final WeakReference<NayaxManager> ref;

        SafeHandler(NayaxManager manager) {
            super(Looper.getMainLooper());
            ref = new WeakReference<>(manager);
        }

        @Override
        public void handleMessage(Message msg) {
            NayaxManager manager = ref.get();
            if (manager != null) {
                manager.handleMsg(msg);
            }
        }
    }
}
