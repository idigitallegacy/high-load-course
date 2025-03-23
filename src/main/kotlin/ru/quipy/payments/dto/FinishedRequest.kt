package ru.quipy.payments.dto

class FinishedRequest(
    paymentStartedAt: Long,
    paymentFinishedAt: Long,
) {
    val processingTime = paymentFinishedAt - paymentStartedAt
}