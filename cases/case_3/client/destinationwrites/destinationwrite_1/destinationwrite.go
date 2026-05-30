package client_destinationwrite_1

// DestinationWriteRule — controls the batch size used by the SQL Server writer.
//
// Default: 1,000 rows per INSERT batch (MSSQL performs well with 1K chunks).
// The check function is evaluated every 30 seconds; it reads a runtime config
// from AuxDB.write_tune_config to allow operators to slow down or speed up
// ingestion without restarting the pipeline.

import (
	"context"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

const (
	defaultBatchSize = 1_000
	checkInterval    = 30 * time.Second
)

func DestinationWriteRule(param *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch:      defaultBatchSize,
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: adjustBatchSize,
	}, nil
}

func adjustBatchSize(props *models.CustomDestinationWriteCheckProps) error {
	conn, err := ulib.GetAuxPostgresConn(props.AuxiliaryDBConnMap)
	if err != nil {
		return nil // Can't reach AuxDB — keep current batch size.
	}
	defer conn.Close(context.Background())

	var batchSize int
	err = conn.QueryRow(context.Background(),
		`SELECT batch_size FROM write_tune_config WHERE pipeline_name = $1 LIMIT 1`,
		props.PipelineName,
	).Scan(&batchSize)
	if err != nil || batchSize <= 0 {
		return nil // Config row not found — keep current.
	}

	props.State.SetDestinationWriteBatchSize(batchSize)
	return nil
}
