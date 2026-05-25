// This is a root bridge file
// All the pipelines get started from here
// Each time a go channel comes in and uses switch condition to trigger one pipeline
package client_pipeline_pipeline_1

import (
	client_source_entity "etlfunnel/execution/client/connectors/connector_44/iso_entity_121"
	client_destination_entity "etlfunnel/execution/client/connectors/connector_32/iso_entity_111"
	client_transformer_61 "etlfunnel/execution/client/transformers/transformer_61"
	client_checkpoint_5 "etlfunnel/execution/client/checkpoints/checkpoint_5"
	client_backlog_5 "etlfunnel/execution/client/backlogs/backlog_5"
	client_terminate_5 "etlfunnel/execution/client/terminates/terminate_5"
	"etlfunnel/execution/contexts"
	"etlfunnel/execution/core/destination"
	"etlfunnel/execution/core/source"
	"etlfunnel/execution/global"
	"etlfunnel/execution/logger"
	"etlfunnel/execution/models"
	"fmt"
	"sync/atomic"
	"time"
	"go.uber.org/zap"
)

type pipelineContext struct {
	pcm                     *contexts.PipelineContextManager
	runtimeState            *contexts.PipelineRuntimeState
	log                     logger.PipelineLogger
	dbConnector             *models.PipelineDBConnectors
	clientSourceBridge      *client_source_entity.IUseConnector
	clientDestinationBridge *client_destination_entity.IUseConnector
	checkpointFunc          func(*models.CheckpointProps) (*models.CheckpointTune, error)
	backlogFunc             func(*models.BacklogProps) (*models.BacklogTune, error)
	transformerParam        models.TransformerProps
}

func PerformOperations(pcm *contexts.PipelineContextManager, producer source.ISourceRecordsProducer, consumer destination.IDestinationRecordsConsumer, pipelineDBConnector *models.PipelineDBConnectors) {
	_logger := logger.Pipeline(pcm.GetContext())
	runtimeState := contexts.NewPipelineRuntimeState(pcm, _logger)
	_logger.Debug("starting pipeline")

	pipeline, err := initializePipeline(pcm, runtimeState, _logger, producer, consumer, pipelineDBConnector)
	if err != nil {
		_logger.Error("Pipeline initialization failed", zap.Error(err))
		return
	}
	if err := runtimeState.RegisterTermination(client_terminate_5.TerminateRule); err != nil {
		_logger.Error("Failed to register termination rule", zap.Error(err))
		return
	}

	reader, err := producer.ProduceRecords(&source.ExecutorContext{
		PCM:                 pcm,
		State:               pipeline.runtimeState,
		PipelineDBConnector: pipelineDBConnector,
		CaptureMethod:       pipelineDBConnector.CaptureMethod,
	})
	if err != nil {
		_logger.Error("Source producer failed to produce records", zap.Error(err))
		return
	}
	if reader == nil {
		_logger.Error("Source producer returned nil reader")
		return
	}

	processRecords(pipeline, reader, consumer)

	flushPipeline(pipeline, reader, consumer)

	if reader.Cleanup != nil {
		if err := reader.Cleanup(); err != nil {
			_logger.Error("Source cleanup failed", zap.Error(err))
		}
	}

	_logger.Debug("pipeline complete")
}

func initializePipeline(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, producer source.ISourceRecordsProducer, consumer destination.IDestinationRecordsConsumer, pipelineDBConnector *models.PipelineDBConnectors) (*pipelineContext, error) {
	pipeline := &pipelineContext{
		pcm:                     pcm,
		runtimeState:            runtimeState,
		log:                     _logger,
		dbConnector:             pipelineDBConnector,
		clientSourceBridge:      &client_source_entity.IUseConnector{},
		clientDestinationBridge: &client_destination_entity.IUseConnector{},
	}
	pipeline.checkpointFunc = client_checkpoint_5.Checkpoint
	pipeline.backlogFunc = client_backlog_5.Backlog

	if producer == nil {
		return nil, fmt.Errorf("source engine is not implemented")
	}

	if consumer == nil {
		return nil, fmt.Errorf("destination engine is not implemented")
	}

	if err := producer.Init(pcm, pipelineDBConnector.Source, pipeline.clientSourceBridge); err != nil {
		return nil, err
	}

	if err := consumer.Init(pcm, pipelineDBConnector.Destination, pipeline.clientDestinationBridge); err != nil {
		return nil, err
	}
	pipeline.transformerParam = models.TransformerProps{
		State:              pipeline.runtimeState,
		Record:             nil,
		SourceDBConn:       pipeline.dbConnector.Source,
		DestDBConn:         pipeline.dbConnector.Destination,
		AuxiliaryDBConnMap: pipeline.dbConnector.AuxilaryHub,
	}

	return pipeline, nil
}

func processRecords(pipeline *pipelineContext, reader *models.IReadByImpl, consumer destination.IDestinationRecordsConsumer) {
	var totalMessages atomic.Uint64
	startTime := time.Now()
	var lastMessageAt atomic.Value
	lastMessageAt.Store(startTime)
	defer pipeline.runtimeState.StopControlPlane()
	for {
		select {
		case <-pipeline.pcm.GetContext().Done():
			pipeline.log.Debug("context cancelled, stopping record processing")
			return

		case <-pipeline.runtimeState.TerminationChan():
			if should, reason := pipeline.runtimeState.EvaluateTermination(totalMessages.Load(), lastMessageAt.Load().(time.Time), startTime); should {
				pipeline.log.Info("Termination condition met: " + reason)
				return
			}

		case <-pipeline.runtimeState.DestinationWriteTuneChan():
			pipeline.runtimeState.EvaluateDestinationWriteTune(totalMessages.Load(), lastMessageAt.Load().(time.Time))

		case record, ok := <-reader.Channel:
			if !ok {
				return
			}
			if global.IsConnectionErrorRecord(record) {
				pipeline.pcm.NotifyConnectionError(fmt.Errorf("connection error"))
				return
			}
			if record == nil {
				continue
			}
			totalMessages.Add(1)
			lastMessageAt.Store(time.Now())
			transformedRecord, err := applyTransformations(record.Data, pipeline)
			if err != nil {
				pipeline.log.Error("Record transformation failed", zap.Error(err))
				if action := handleBacklog(pipeline, []*models.Record{record}, models.FailureStageTransform, err); action != models.ActionContinue {
					return
				}
				continue
			}
			if transformedRecord == nil {
				continue
			}
			record.Data = transformedRecord
			if action := consumeRecord(pipeline, consumer, record, reader); action != models.ActionContinue {
				return
			}
		}
	}
}

func applyTransformations(record map[string]any, pipeline *pipelineContext) (map[string]any, error) {
	transformedRecord := record
	var err error
	var result map[string]any
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_61.Transformer(&pipeline.transformerParam)
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	return transformedRecord, nil
}

func consumeRecord(pipeline *pipelineContext, consumer destination.IDestinationRecordsConsumer, record *models.Record, reader *models.IReadByImpl) models.PipelineAction {
	status, records, err := consumer.ConsumeRecords(pipeline.pcm, pipeline.runtimeState, record, pipeline.dbConnector)
	if err != nil {
		pipeline.log.Error("Failed to consume record at destination", zap.Error(err))
		return handleBacklog(pipeline, records, models.FailureStageDestination, err)
	}

	if status == models.RecordPushCommitted {
		if reader.CommitHook != nil {
			if action := reader.CommitHook(records); action != models.ActionContinue {
				return models.ActionStop
			}
		}
		return handleCheckpoint(pipeline, records)
	}

	return models.ActionContinue
}

func handleBacklog(pipeline *pipelineContext, records []*models.Record, failureStage models.FailureStage, cause error) models.PipelineAction {
	if pipeline.backlogFunc == nil {
		return models.ActionContinue
	}

	backlogParam := models.BacklogProps{
		State:              pipeline.runtimeState,
		FailureStage:       failureStage,
		Err:                cause,
		Records:            extractRecordData(records),
		SourceDBConn:       pipeline.dbConnector.Source,
		DestDBConn:         pipeline.dbConnector.Destination,
		AuxiliaryDBConnMap: pipeline.dbConnector.AuxilaryHub,
	}

	backlogResponse, err := pipeline.backlogFunc(&backlogParam)
	if err != nil {
		pipeline.log.Error("Backlog handler failed", zap.Error(err))
		if pipeline.dbConnector.Destination.IsConnectionError(err) {
			pipeline.pcm.NotifyConnectionError(err)
			return models.ActionStop
		}
		return backlogResponse.Action
	}

	if backlogResponse != nil && backlogResponse.Action == models.ActionStop {
		pipeline.log.Debug("backlog handler requested pipeline stop")
	}

	return backlogResponse.Action
}

func handleCheckpoint(pipeline *pipelineContext, records []*models.Record) models.PipelineAction {
	if pipeline.checkpointFunc == nil {
		return models.ActionContinue
	}

	checkpointParam := models.CheckpointProps{
		State:              pipeline.runtimeState,
		Records:            extractRecordData(records),
		SourceDBConn:       pipeline.dbConnector.Source,
		DestDBConn:         pipeline.dbConnector.Destination,
		AuxiliaryDBConnMap: pipeline.dbConnector.AuxilaryHub,
	}

	checkpointResponse, err := pipeline.checkpointFunc(&checkpointParam)
	if err != nil {
		pipeline.log.Error("Checkpoint handler failed", zap.Error(err))
		if pipeline.dbConnector.Source.IsConnectionError(err) {
			pipeline.pcm.NotifyConnectionError(err)
			return models.ActionStop
		}
		return checkpointResponse.Action
	}

	if checkpointResponse != nil && checkpointResponse.Action == models.ActionStop {
		pipeline.log.Debug("checkpoint handler requested pipeline stop")
	}

	return checkpointResponse.Action
}

func flushPipeline(pipeline *pipelineContext, reader *models.IReadByImpl, consumer destination.IDestinationRecordsConsumer) {
	status, records, err := consumer.Flush(pipeline.pcm)

	if err != nil {
		pipeline.log.Error("Destination flush failed", zap.Error(err))
		handleBacklog(pipeline, records, models.FailureStageDestination, err)
	}

	if status == models.RecordPushCommitted {
		if reader.CommitHook != nil {
			reader.CommitHook(records)
		}
		handleCheckpoint(pipeline, records)
	}
}

func extractRecordData(records []*models.Record) []map[string]any {
	dataRecords := make([]map[string]any, len(records))
	for i, record := range records {
		dataRecords[i] = record.Data
	}
	return dataRecords
}