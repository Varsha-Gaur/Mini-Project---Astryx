"""
dp_module.py
============
Differential Privacy Module
----------------------------
Provides a production-ready DifferentialPrivacyModule that applies calibrated
Laplace and Gaussian noise mechanisms to MeterReading objects.

Design goals
------------
* Real implementation uses IBM diffprivlib (Laplace / Gaussian mechanisms).
* When diffprivlib is unavailable the module falls back to a pure-numpy
  implementation that is mathematically equivalent for research purposes.
* Tracks a per-session privacy budget ledger (epsilon / delta accounting).
* Thread-safe: all state mutations are protected by a threading.Lock.

Privacy pipeline position
-------------------------
    MeterReading  ──►  DifferentialPrivacyModule  ──►  (noised) MeterReading
                                                              │
                                                              ▼
                                                   HomomorphicEncryptionModule

Usage
-----
    from dp_module import DifferentialPrivacyModule, DPConfig

    dp = DifferentialPrivacyModule(DPConfig(epsilon=1.0, mechanism="laplace"))
    noised_reading = dp.apply(reading)
    print(dp.budget_report())

"""
