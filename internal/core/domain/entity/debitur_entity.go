package entity

import "time"

type DebiturPrewarmRecord struct {
	NIK               string    `json:"nik"`
	Nama              string    `json:"nama"`
	NomorRekening     string    `json:"nomor_rekening"`
	NomorTelepon      string    `json:"nomor_telepon"`
	UnitKerjaID       string    `json:"unit_kerja_id"`
	ExistingBakiDebet int64     `json:"existing_baki_debet"`
	OriginalPlafond   int64     `json:"original_plafond"`
	MaxSuplesiAmount  int64     `json:"max_suplesi_amount"`
	AllowedTenors     []int32   `json:"allowed_tenors"`
	Token             string    `json:"token,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

type PrewarmSummary struct {
	BatchID       string   `json:"batch_id"`
	TotalReceived int64    `json:"total_received"`
	TotalCached   int64    `json:"total_cached"`
	TotalFailed   int64    `json:"total_failed"`
	DurationMs    int64    `json:"duration_ms"`
	FailedNIKs    []string `json:"failed_niks,omitempty"`
}

type DebiturOfferingResponse struct {
	Token             string  `json:"token"`
	Nama              string  `json:"nama"`
	NomorRekeningMask string  `json:"nomor_rekening_masked"`
	MaxSuplesiAmount  int64   `json:"max_suplesi_amount"`
	ExistingBakiDebet int64   `json:"existing_baki_debet"`
	AllowedTenors     []int32 `json:"allowed_tenors"`
	Expiry            string  `json:"expiry_time"`
}
