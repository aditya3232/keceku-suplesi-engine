#!/usr/bin/env bash

# ==============================================================================
# Keceku Suplesi Engine - cURL Collection untuk Import ke Bruno / Terminal
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Health Check (REST API)
# Bruno Import: Menu -> Import -> cURL (atau paste langsung ke address bar)
# ------------------------------------------------------------------------------
curl -X GET "http://localhost:8090/health" \
  -H "Accept: application/json"

# ------------------------------------------------------------------------------
# 2. Prewarm Batch Debitur (REST API)
# Mengirimkan batch chunk debitur via HTTP POST
# ------------------------------------------------------------------------------
curl -X POST "http://localhost:8090/api/v1/prewarm" \
  -H "Content-Type: application/json" \
  -d '{
    "batch_id": "BATCH-BRUNO-001",
    "data": [
      {
        "nik": "3201019999990001",
        "nama": "Budi Santoso (Bruno Pilot)",
        "nomor_rekening": "012301999999",
        "nomor_telepon": "628129999999",
        "unit_kerja_id": "UNIT-BRI-0421",
        "existing_baki_debet": 20000000,
        "original_plafond": 50000000,
        "max_suplesi_amount": 30000000,
        "allowed_tenors": [12, 24, 36]
      },
      {
        "nik": "3201019999990002",
        "nama": "Siti Aminah (Bruno Pilot)",
        "nomor_rekening": "012301999998",
        "nomor_telepon": "628129999998",
        "unit_kerja_id": "UNIT-BRI-0421",
        "existing_baki_debet": 10000000,
        "original_plafond": 40000000,
        "max_suplesi_amount": 30000000,
        "allowed_tenors": [12, 24]
      }
    ]
  }'

# ------------------------------------------------------------------------------
# 3. Get Token by NIK (REST API)
# Mengambil token yang sudah di-generate untuk NIK tertentu
# ------------------------------------------------------------------------------
curl -X GET "http://localhost:8090/api/v1/token/3201016666660001" \
  -H "Accept: application/json"

# ------------------------------------------------------------------------------
# 4. Get Debitur Offering by Token (REST API - Simulasi Klik Link WhatsApp)
# Membuka tautan penawaran suplesi dengan target latensi < 5ms
# ------------------------------------------------------------------------------
curl -X GET "http://localhost:8090/s/6f68761214756dae27a1c7312bf8f765" \
  -H "Accept: application/json"

# ------------------------------------------------------------------------------
# 5. Get Cache Statistics (REST API)
# Melihat total key yang tersimpan di Redis
# ------------------------------------------------------------------------------
curl -X GET "http://localhost:8090/api/v1/stats" \
  -H "Accept: application/json"

# ==============================================================================
# gRPC Calls (Menggunakan grpcurl - CLI standar ekuivalen cURL untuk gRPC)
# Port gRPC: localhost:50051
# ==============================================================================

# A. List Service gRPC (via Reflection)
grpcurl -plaintext localhost:50051 list

# B. Describe LoanPreWarmService
grpcurl -plaintext localhost:50051 describe keceku.loan.v1.LoanPreWarmService

# C. StreamPreWarmDebitur (Client Streaming gRPC Invocation)
grpcurl -plaintext \
  -proto proto/keceku/loan/v1/loan_prewarm.proto \
  -d '{
    "batch_id": "BATCH-GRPCURL-001",
    "data": [
      {
        "nik": "3201016666660001",
        "nama": "Budi Grpcurl",
        "nomor_rekening": "012301666666",
        "nomor_telepon": "628126666666",
        "unit_kerja_id": "UNIT-BRI-001",
        "existing_baki_debet": 20000000,
        "original_plafond": 50000000,
        "max_suplesi_amount": 30000000,
        "allowed_tenors": [12, 24, 36]
      }
    ]
  }' \
  localhost:50051 \
  keceku.loan.v1.LoanPreWarmService/StreamPreWarmDebitur
