package ru.quipy.payments.dto

import kotlinx.coroutines.runBlocking
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.time.Duration
import java.util.*
import kotlin.math.abs

open class InFlightRequest(
    val previousStateRequest: PendingRequest,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val url: String,
    private val timeout: Long,
    private val exceptionHandler: (paymentRequest: PendingRequest, e: Exception) -> Unit
) : Comparable<InFlightRequest>, Runnable {
    override fun compareTo(other: InFlightRequest): Int {
        if (equalsWithThreshold(timeout, other.timeout, 100)) {
            return previousStateRequest.compareTo(other.previousStateRequest)
        }

        return timeout.compareTo(other.timeout)
    }

    var submissionTime: Long = 0
    var completionTime: Long = 0

    override fun run() {
        try {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(previousStateRequest.paymentId) {
                it.logSubmission(
                    success = true,
                    previousStateRequest.transactionId,
                    now(),
                    Duration.ofMillis(now() - previousStateRequest.paymentStartedAt)
                )
            }

            runBlocking {
                submissionTime = now()
                val body = previousStateRequest.call(url, timeout)
                completionTime = now()

                paymentESService.update(previousStateRequest.paymentId) {
                    it.logProcessing(body.result, now(), previousStateRequest.transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            exceptionHandler(previousStateRequest, e)
        }
    }

    private fun equalsWithThreshold(a: Long, b: Long, threshold: Long): Boolean {
        return abs(a.compareTo(b)) < threshold
    }
}