# Panduan Integrasi Bruno 4.2.1 (REST API & gRPC)

Koleksi API untuk **Keceku Suplesi Engine** telah disiapkan dalam 2 cara praktis:
1. **Membuka Koleksi Bruno Langsung (`bruno/`)**: Mendukung REST API & native gRPC request sekaligus.
2. **Import cURL**: Setiap perintah cURL di bawah dapat langsung diimpor ke Bruno melalui fitur **Import -> cURL** atau di-*paste* ke address bar Bruno.

---

## Cara 1: Buka Koleksi Bruno (Paling Direkomendasikan ⭐)

Folder [`bruno/`](./bruno) sudah dikonfigurasi penuh dengan struktur koleksi resmi Bruno.

1. Buka aplikasi **Bruno**.
2. Klik menu **Open Collection** di sisi kiri atas.
3. Pilih folder: `/home/xylo/Documents/adit/devbox/keceku-suplesi-engine/bruno`.
4. Di pojok kanan atas Bruno, pilih environment **`local`**.
5. Anda akan melihat dua folder utama:
   - **`REST-API/`**:
     - `01. Health Check` (`GET {{baseUrl}}/health`)
     - `02. Prewarm Batch Debitur` (`POST {{baseUrl}}/api/v1/prewarm`)
     - `03. Get Token by NIK` (`GET {{baseUrl}}/api/v1/token/{{sampleNik}}`)
     - `04. Get Debitur Offer by Token` (`GET {{baseUrl}}/s/{{sampleToken}}`)
     - `05. Get Cache Statistics` (`GET {{baseUrl}}/api/v1/stats`)
   - **`gRPC/`**:
     - `Stream Pre-Warm Debitur` (`keceku.loan.v1.LoanPreWarmService/StreamPreWarmDebitur` di `127.0.0.1:50051`)

Tinggal klik **Send** pada request mana pun untuk melihat respons langsung!

---

## Cara 2: Daftar cURL untuk Fitur "Import cURL" di Bruno

Di Bruno, klik **... (titik tiga)** pada koleksi -> pilih **Import** -> **cURL**, lalu paste salah satu perintah di bawah ini:

### 1. Health Check (Engine & Redis Connection)
```bash
curl -X GET "http://localhost:8090/health" \
  -H "Accept: application/json"
```

### 2. Prewarm Batch Debitur (Injeksi Data Debitur ke Redis)
```bash
curl -X POST "http://localhost:8090/api/v1/prewarm" \
  -H "Content-Type: application/json" \
  -d '{
    "batch_id": "BATCH-BRUNO-DEMO-001",
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
```

### 3. Get Token by NIK (Melihat Token Debitur Tertentu)
```bash
curl -X GET "http://localhost:8090/api/v1/token/3201016666660001" \
  -H "Accept: application/json"
```

### 4. Get Debitur Offer (Simulasi Klik Tautan WhatsApp)
```bash
curl -X GET "http://localhost:8090/s/6f68761214756dae27a1c7312bf8f765" \
  -H "Accept: application/json"
```

### 5. Get Cache Statistics (Total Data di Redis)
```bash
curl -X GET "http://localhost:8090/api/v1/stats" \
  -H "Accept: application/json"
```

---

## ⚡ gRPC Streaming Calling Guide

Karena protokol gRPC menggunakan HTTP/2 biner, Bruno menyediakan dukungan native gRPC (tersedia pada request `gRPC/Stream Pre-Warm Debitur` di koleksi `bruno/`).

Jika ingin menjalankan gRPC langsung via CLI layaknya cURL (menggunakan `grpcurl`):

### A. List Service via gRPC Server Reflection
```bash
grpcurl -plaintext localhost:50051 list
```

### B. Describe Schema Service
```bash
grpcurl -plaintext localhost:50051 describe keceku.loan.v1.LoanPreWarmService
```

### C. Stream Batch Debitur ke gRPC Server
```bash
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
```
