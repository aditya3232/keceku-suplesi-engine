package cmd

import (
	"github.com/aditya3232/keceku-suplesi-engine/internal/app"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:     "start",
	Aliases: []string{"server"},
	Short:   "Start Keceku Suplesi Server (gRPC + Redis + HTTP Health Check)",
	Long:    `Menjalankan server gRPC LoanPreWarmService dan server HTTP Fiber untuk simulasi URL token suplesi`,
	Run: func(cmd *cobra.Command, args []string) {
		app.RunServer()
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
}
