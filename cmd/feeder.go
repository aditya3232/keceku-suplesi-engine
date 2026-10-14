package cmd

import (
	"github.com/aditya3232/keceku-suplesi-engine/internal/app"
	"github.com/spf13/cobra"
)

var (
	feederCount     int
	feederChunkSize int
	feederBatchID   string
	feederTarget    string
)

var feederCmd = &cobra.Command{
	Use:     "feeder",
	Aliases: []string{"feed", "batch"},
	Short:   "Run Batch Feeder Client Streaming",
	Long:    `Mengalirkan ribuan data debitur kandidat suplesi ke gRPC Loan Service via Client Streaming`,
	Run: func(cmd *cobra.Command, args []string) {
		_, _ = app.RunFeeder(feederBatchID, feederCount, feederChunkSize, feederTarget)
	},
}

func init() {
	feederCmd.Flags().IntVarP(&feederCount, "count", "n", 100000, "Total debitur kandidat yang akan dialirkan")
	feederCmd.Flags().IntVarP(&feederChunkSize, "chunk-size", "c", 500, "Ukuran batch chunk per pesan gRPC")
	feederCmd.Flags().StringVarP(&feederBatchID, "batch-id", "b", "", "ID batch unik (opsional)")
	feederCmd.Flags().StringVarP(&feederTarget, "target", "t", "localhost:50051", "Alamat gRPC server target")

	rootCmd.AddCommand(feederCmd)
}
