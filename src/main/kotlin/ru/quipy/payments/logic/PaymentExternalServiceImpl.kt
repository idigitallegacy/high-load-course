package ru.quipy.payments.logic

import kotlinx.coroutines.*
import java.util.concurrent.Semaphore
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.common.utils.exceptions.RETRIABLE_PROCESSING_FAIL_REASONS
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.config.SUCCESS_QUANTILE
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.dto.FinishedRequest
import ru.quipy.payments.dto.InFlightRequest
import ru.quipy.payments.dto.PendingRequest
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val inFlightRequestsScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(contentType = null)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestsQueue = PriorityBlockingQueue<PendingRequest>(parallelRequests)
    private val outgoingRateLimiter = CompositeRateLimiter(SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L)), LeakingBucketRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L), rateLimitPerSec))
    private val inFlightRequestsLimiter = Semaphore(parallelRequests)

    private val externalSystemAnalyzer = ExternalSystemAnalyzer()

    private val coreExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        PriorityBlockingQueue(),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        val pendingRequest =
            PendingRequest(transactionId, paymentId, amount, paymentStartedAt, getMaxRetries(amount, deadline), deadline, accountName)
        requestsQueue.add(pendingRequest)
    }

    override fun submitPayments() {
        val paymentRequest = requestsQueue.poll()

        if (paymentRequest === null) {
            return
        }

        val timeout = getTimeout()
        val url = "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=${paymentRequest.transactionId}&paymentId=${paymentRequest.paymentId}&amount=${paymentRequest.amount}&timeout=PT${"%.3f".format(timeout.toDouble() / 1000.00)}s"

        val futureTask = CompletableFuture<Unit>()
        val request = object : InFlightRequest(paymentRequest, paymentESService, url, timeout, ::handleException) {
            private fun coreFunc() {
                runBlocking {
                    withContext(Dispatchers.Default) {
                        withContext(inFlightRequestsScope.coroutineContext) {
                            outgoingRateLimiter.tickBlocking()
                            inFlightRequestsLimiter.acquireUninterruptibly()
                        }
                    }

                    try {
                        super.run()
                    } finally {
                        inFlightRequestsLimiter.release()
                    }
                }
            }

            override fun run() {
                try {
                    futureTask.complete(coreFunc())
                } catch (ex: Exception) {
                    futureTask.completeExceptionally(ex)
                } finally {
                    externalSystemAnalyzer.addFinished(FinishedRequest(super.submissionTime, super.completionTime))
                }
            }
        }

        coreExecutor.execute(request)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Boolean {
        if (deadline.toDouble() < now().toDouble() + (requestsQueue.size + 1).toDouble() / selectMaxRps() * 1000 + getTimeout()) {
            throw IllegalStateException("Time limits for $accountName breached")
        }

        return true
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

            is RequestProcessingException -> {
                logger.error(
                    "[$accountName] Request processing failed for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}. Reason: ${e.failReason}. Message: ${e.message}"
                )

                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }

                if (RETRIABLE_PROCESSING_FAIL_REASONS.contains(e.failReason)) {
                    paymentRequest.setRetryAfter((paymentRequest.deadline - now()) / (paymentRequest.maxRetries.get() - paymentRequest.getRetries()))
                    requestsQueue.add(paymentRequest)
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

    private fun cleanup() {
        while (requestsQueue.peek() !== null && requestsQueue.peek().deadline < now()) {
            val paymentRequest = requestsQueue.poll()

            paymentESService.update(paymentRequest.paymentId) {
                it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Timeout")
            }
        }
    }

    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            submitPayments()
            cleanup()
            Thread.sleep(10L)
        }
    }

    private fun selectMaxRps(): Double {
        val ownRps = rateLimitPerSec.toDouble()
        val providerRps = 1 / requestAverageProcessingTime.toSeconds().toDouble() * rateLimitPerSec.toDouble()

        return min(ownRps, providerRps)
    }

    private fun getMaxRetries(amount: Int, deadline: Long): AtomicLong {
        val maxRetriesByAmount = (amount / price()).toLong()
        val maxRetriesByDeadline = (deadline - now()) / requestAverageProcessingTime.toMillis()

        return AtomicLong(min(maxRetriesByAmount, maxRetriesByDeadline))
    }

    private fun getTimeout(): Long {
        var timeout = externalSystemAnalyzer.getQuantile(SUCCESS_QUANTILE)?.processingTime ?: requestAverageProcessingTime.toMillis()
        timeout = min(timeout, 2 * requestAverageProcessingTime.toMillis())

        return timeout
    }
}

fun now() = System.currentTimeMillis()