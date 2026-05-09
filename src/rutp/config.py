"""Динамическая конфигурация (из переменных окружения)."""
import os
from .constants import DEFAULT_RTO, MIN_RTO, MAX_RTO

def get_config():
    return {
        "rto": float(os.getenv("RUTP_RTO", DEFAULT_RTO)),
        "min_rto": float(os.getenv("RUTP_MIN_RTO", MIN_RTO)),
        "max_rto": float(os.getenv("RUTP_MAX_RTO", MAX_RTO)),
        "max_retransmits": int(os.getenv("RUTP_MAX_RETRANSMITS", "10")),
    }
