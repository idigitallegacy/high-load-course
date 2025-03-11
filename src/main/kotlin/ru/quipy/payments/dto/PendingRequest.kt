package ru.quipy.payments.dto

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.exceptions.ProcessingFailReason
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.payments.logic.ExternalSysResponse
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

class PendingRequest(
    val transactionId: UUID,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    private val deadline: Long,
    private val accountName: String,
    private val rawRequest: Request,
    private val maxRetries: AtomicLong
) : Comparable<PendingRequest>, Callable<ExternalSysResponse> {
    private val retriesAmount = AtomicLong(0)

    companion object {
        val logger = LoggerFactory.getLogger(PendingRequest::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val client = OkHttpClient.Builder().build()

    override fun compareTo(other: PendingRequest): Int {
        return deadline.compareTo(other.deadline)
    }

    override fun call(): ExternalSysResponse {
        if (retriesAmount.incrementAndGet() >= maxRetries.get()) {
            throw RequestProcessingException(ProcessingFailReason.TOO_MANY_RETRIES, "Too Many Retries")
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            client.newCall(rawRequest).execute().use { response ->
                if (!response.isSuccessful) {
                    throw handleFailedResponse(response)
                }

                val body = safeGetBody(response)

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                if (!body.result) {
                    // successful response with failure
                    throw RequestProcessingException(ProcessingFailReason.UNKNOWN, body.message)
                }

                return body
            }
    }

    private fun safeGetBody(response: Response): ExternalSysResponse {
        try {
            return mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")

            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private fun handleFailedResponse(response: Response): RequestProcessingException {
        val body = safeGetBody(response)

        return when (response.code) {
            429 -> RequestProcessingException(ProcessingFailReason.TOO_MANY_REQUESTS, body.message)
            500 -> RequestProcessingException(ProcessingFailReason.INTERNAL_SERVER_ERROR, body.message)
            502 -> RequestProcessingException(ProcessingFailReason.BAD_GATEWAY, body.message)
            503 -> RequestProcessingException(ProcessingFailReason.SERVICE_UNAVAILABLE, body.message)
            504 -> RequestProcessingException(ProcessingFailReason.GATEWAY_TIMEOUT, body.message)

            400 -> RequestProcessingException(ProcessingFailReason.BAD_REQUEST, body.message)
            401 -> RequestProcessingException(ProcessingFailReason.UNAUTHORIZED, body.message)
            403 -> RequestProcessingException(ProcessingFailReason.FORBIDDEN, body.message)
            404 -> RequestProcessingException(ProcessingFailReason.NOT_FOUND, body.message)
            405 -> RequestProcessingException(ProcessingFailReason.METHOD_NOT_ALLOWED, body.message)

            else -> RequestProcessingException(ProcessingFailReason.UNKNOWN)
        }
    }
}