package com.dawn.nayax;

/**
 * Nayax设备回调接口
 * <p>所有回调均在主线程执行</p>
 */
public interface NayaxCallback {

    /**
     * 设备就绪
     *
     * @param version      控制板版本
     * @param deviceType   外设类型
     * @param currencyCode 货币代码
     * @param minAmount    支持的最小面额
     */
    void onDeviceReady(String version, String deviceType, String currencyCode, float minAmount);

    /**
     * 收款已发起
     */
    void onPaymentStarted();

    /**
     * 收到付款
     *
     * @param payType 支付方式
     * @param amount  实际收款金额
     */
    void onPaymentReceived(String payType, float amount);

    /**
     * 收款完成
     *
     * @param amount 最终收款金额
     */
    void onPaymentCompleted(float amount);

    /**
     * 收款已取消
     */
    void onPaymentCancelled();

    /**
     * 售卖结果
     *
     * @param success 是否成功
     */
    void onSaleResult(boolean success);

    /**
     * 部分付款进度通知
     * <p>在轮询期间收到部分付款金额时回调（金额不足时持续通知）</p>
     *
     * @param currentAmount 当前已收金额
     * @param targetAmount  目标收款金额
     */
    default void onPartialPayment(float currentAmount, float targetAmount) {}

    /**
     * 错误回调
     *
     * @param errorCode 错误码，参见 {@link NayaxManager} 中的 ERROR_* 常量
     * @param message   错误描述
     */
    void onError(int errorCode, String message);

    /**
     * 连接状态变化
     *
     * @param connected 是否已连接
     */
    void onConnectionChanged(boolean connected);
}
