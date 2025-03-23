package ru.quipy.config

// HTTP
const val SUCCESS_QUANTILE = 0.95
const val MAX_RETRIES_LIMIT = 5L

// Hardware
val MAX_THREADS_LIMIT = 2 * Runtime.getRuntime().availableProcessors() * 4 // 2 = threads per CPU; 4 = to get threads queue