package ru.quipy.payments.scheduled

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.quipy.config.SCHEDULED_POLLING_INTERVAL
import ru.quipy.payments.logic.PaymentExternalSystemAdapter

@Service
class ScheduledExecutor(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) {

    @Scheduled(fixedRate = SCHEDULED_POLLING_INTERVAL)
    fun acceptPayments() {
        for (account in paymentAccounts) {
            account.submitPayments()
        }
    }
}