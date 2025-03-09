package ru.quipy.payments.logic

import okhttp3.OkHttpClient
import okhttp3.Request
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.mapper
import java.util.*
import java.util.concurrent.Callable

class PendingRequest(
    val transactionId: UUID,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val accountName: String,
    val rawRequest: Request
) : Comparable<PendingRequest>, Callable<ExternalSysResponse> {
    private val client = OkHttpClient.Builder().build()

    override fun compareTo(other: PendingRequest): Int {
        return deadline.compareTo(other.deadline)
    }

    override fun call(): ExternalSysResponse {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            client.newCall(rawRequest).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                return body
            }
    }
}