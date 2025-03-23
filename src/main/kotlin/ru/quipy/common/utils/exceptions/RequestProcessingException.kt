package ru.quipy.common.utils.exceptions

enum class ProcessingFailReason {
    UNKNOWN,

    // Retriable
    TOO_MANY_REQUESTS,
    INTERNAL_SERVER_ERROR,
    BAD_GATEWAY,
    SERVICE_UNAVAILABLE,
    GATEWAY_TIMEOUT,
    TIMEOUT,

    // Bad ones
    TOO_MANY_RETRIES,
    BAD_REQUEST,
    UNAUTHORIZED,
    FORBIDDEN,
    NOT_FOUND,
    METHOD_NOT_ALLOWED,
}

val RETRIABLE_PROCESSING_FAIL_REASONS = setOf(ProcessingFailReason.UNKNOWN, ProcessingFailReason.TOO_MANY_REQUESTS, ProcessingFailReason.INTERNAL_SERVER_ERROR, ProcessingFailReason.BAD_GATEWAY, ProcessingFailReason.SERVICE_UNAVAILABLE, ProcessingFailReason.GATEWAY_TIMEOUT, ProcessingFailReason.TIMEOUT)

class RequestProcessingException(val failReason: ProcessingFailReason, message: String? = null) : Exception(message) {
}