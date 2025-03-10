package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestsQueue = PriorityBlockingQueue<PendingRequest>(parallelRequests)
    private val rateLimiter =
        LeakingBucketRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1), parallelRequests)

    private val coreExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-submission-executor")
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${paymentId}&amount=${amount}")
            post(emptyBody)
        }.build()

        val pendingRequest =
            PendingRequest(transactionId, paymentId, amount, paymentStartedAt, deadline, accountName, request)
        requestsQueue.add(pendingRequest)
    }

    override fun submitPayments() {
        for (i in 1..rateLimitPerSec) {
            val paymentRequest = requestsQueue.poll()

            if (paymentRequest === null) {
                return
            }

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentRequest.paymentId) {
                it.logSubmission(
                    success = true,
                    paymentRequest.transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentRequest.paymentStartedAt)
                )
            }

            coreExecutor.submit {
                try {
                    val body = paymentRequest.call()

                    paymentESService.update(paymentRequest.paymentId) {
                        it.logProcessing(body.result, now(), paymentRequest.transactionId, reason = body.message)
                    }
                } catch (e: Exception) {
                    handleException(paymentRequest, e)
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(amount: Int, deadline: Long): Boolean {
        if (!rateLimiter.tick()) {
            return false
        }

        return now() + requestsQueue.size / rateLimitPerSec * 1000 + requestAverageProcessingTime.toMillis() < deadline
    }

    private fun handleException(paymentRequest: PendingRequest, e: Exception) {
        when (e) {
            is SocketTimeoutException -> {
                logger.error(
                    "[$accountName] Payment timeout for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}",
                    e
                )
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error(
                    "[$accountName] Payment failed for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}",
                    e
                )

                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }
            }
        }
    }
}

public fun now() = System.currentTimeMillis()