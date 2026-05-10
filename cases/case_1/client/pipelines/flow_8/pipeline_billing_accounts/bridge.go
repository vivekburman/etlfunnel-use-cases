// This is a root bridge file
// All the pipelines get started from here
// Each time a go channel comes in and uses switch condition to trigger one pipeline
package client_pipeline_billing_accounts

import (
	client_source_entity "etlfunnel/execution/client/connectors/connector_15/iso_entity_49"
	client_destination_entity "etlfunnel/execution/client/connectors/connector_12/iso_entity_65"
	client_transformer_25 "etlfunnel/execution/client/transformers/transformer_25"
	client_transformer_16 "etlfunnel/execution/client/transformers/transformer_16"
	client_transformer_27 "etlfunnel/execution/client/transformers/transformer_27"
	client_transformer_17 "etlfunnel/execution/client/transformers/transformer_17"
	client_transformer_18 "etlfunnel/execution/client/transformers/transformer_18"
	client_transformer_19 "etlfunnel/execution/client/transformers/transformer_19"
	client_transformer_21 "etlfunnel/execution/client/transformers/transformer_21"
	client_transformer_46 "etlfunnel/execution/client/transformers/transformer_46"
	client_checkpoint_3 "etlfunnel/execution/client/checkpoints/checkpoint_3"
	client_backlog_3 "etlfunnel/execution/client/backlogs/backlog_3"
	client_terminate_3 "etlfunnel/execution/client/terminates/terminate_3"
	client_destinationwrite_3 "etlfunnel/execution/client/destinationwrites/destinationwrite_3"
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
	clientSourceBridge      *client_source_entity.IUseConnector
	clientDestinationBridge *client_destination_entity.IUseConnector
	checkpointFunc          func(*models.CheckpointProps) (*models.CheckpointTune, error)
	backlogFunc             func(*models.BacklogProps) (*models.BacklogTune, error)
	transformerParam        models.TransformerProps
}

func PerformOperations(pcm *contexts.PipelineContextManager, flowDefinition *models.FlowDefinitionImpl, definition *models.FlowPipelineDefinitionImpl, producer source.ISourceRecordsProducer, consumer destination.IDestinationRecordsConsumer, pipelineDBConnector *models.PipelineDBConnectors) {
	_logger := logger.Pipeline(pcm.GetContext())
	runtimeState := contexts.NewPipelineRuntimeState(pcm, _logger)
	_logger.Debug("starting pipeline")

	pipeline, err := initializePipeline(pcm, runtimeState, producer, consumer, pipelineDBConnector)
	if err != nil {
		_logger.Error("Pipeline initialization failed", zap.Error(err))
		return
	}
	if err := runtimeState.RegisterTermination(client_terminate_3.TerminateRule); err != nil {
		_logger.Error("Failed to register termination rule", zap.Error(err))
		return
	}
	if err := runtimeState.RegisterDestinationWriteRule(client_destinationwrite_3.DestinationWriteRule); err != nil {
		_logger.Error("Failed to register batch tune", zap.Error(err))
		return
	}

	reader, err := producer.ProduceRecords(pcm, runtimeState, flowDefinition, pipelineDBConnector, pipelineDBConnector.Source, pipeline.clientSourceBridge)
	if err != nil {
		_logger.Error("Source producer failed to produce records", zap.Error(err))
		return
	}
	if reader == nil {
		_logger.Error("Source producer returned nil reader")
		return
	}

	processRecords(pcm, runtimeState, _logger, reader, consumer, pipeline, pipelineDBConnector)

	flushPipeline(pcm, runtimeState, _logger, reader, consumer, pipeline, pipelineDBConnector)

	if reader.Cleanup != nil {
		if err := reader.Cleanup(); err != nil {
			_logger.Error("Source cleanup failed", zap.Error(err))
		}
	}

	_logger.Debug("pipeline complete")
}

func initializePipeline(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, producer source.ISourceRecordsProducer, consumer destination.IDestinationRecordsConsumer, pipelineDBConnector *models.PipelineDBConnectors) (*pipelineContext, error) {
	pipeline := &pipelineContext{
		clientSourceBridge:      &client_source_entity.IUseConnector{},
		clientDestinationBridge: &client_destination_entity.IUseConnector{},
	}
	pipeline.checkpointFunc = client_checkpoint_3.Checkpoint
	pipeline.backlogFunc = client_backlog_3.Backlog

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
		State:              runtimeState,
		Record:             nil,
		SourceDBConn:       pipelineDBConnector.Source,
		DestDBConn:         pipelineDBConnector.Destination,
		AuxiliaryDBConnMap: pipelineDBConnector.AuxilaryHub,
	}
	
	return pipeline, nil
}

func processRecords(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, reader *models.IReadByImpl, consumer destination.IDestinationRecordsConsumer, pipeline *pipelineContext, pipelineDBConnector *models.PipelineDBConnectors) {
	var totalMessages atomic.Uint64
	startTime := time.Now()
	var lastMessageAt atomic.Value
	lastMessageAt.Store(startTime)
	defer runtimeState.StopControlPlane()
	for {
		select {
		case <-pcm.GetContext().Done():
			_logger.Debug("context cancelled, stopping record processing")
			return

		case <-runtimeState.TerminationChan():
			if should, reason := runtimeState.EvaluateTermination(totalMessages.Load(), lastMessageAt.Load().(time.Time), startTime); should {
				_logger.Info("Termination condition met: " + reason)
				return
			}

		case <-runtimeState.DestinationWriteTuneChan():
			runtimeState.EvaluateDestinationWriteTune(totalMessages.Load(), lastMessageAt.Load().(time.Time))

		case record, ok := <-reader.Channel:
			if !ok {
				// Channel closed, Termination gracefully
				return
			}
			if global.IsConnectionErrorRecord(record) {
				pcm.NotifyConnectionError(fmt.Errorf("connection error"))
				return
			}
			if record == nil {
				continue
			}
			totalMessages.Add(1)
			lastMessageAt.Store(time.Now())
			transformedRecord, err := applyTransformations(record.Data, pipeline)
			if err != nil {
				_logger.Error("Record transformation failed", zap.Error(err))
				if action := handleBacklog(pcm, runtimeState, _logger, []*models.Record{record}, models.FailureStageTransform, err, pipeline, pipelineDBConnector); action != models.ActionContinue {
					return
				}
				continue
			}
			if transformedRecord == nil {
				continue
			}
			record.Data = transformedRecord
			if action := consumeRecord(pcm, runtimeState, _logger, consumer, record, reader, pipeline, pipelineDBConnector); action != models.ActionContinue {
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
	result, err = client_transformer_25.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_16.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_27.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_17.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_18.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_19.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_21.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	pipeline.transformerParam.Record = transformedRecord
	result, err = client_transformer_46.Transformer(&pipeline.transformerParam)		
	if err != nil || result == nil {
		return nil, err
	}
	transformedRecord = result
	return transformedRecord, nil
}

func consumeRecord(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, consumer destination.IDestinationRecordsConsumer, record *models.Record, reader *models.IReadByImpl, pipeline *pipelineContext, pipelineDBConnector *models.PipelineDBConnectors) models.PipelineAction {
	status, records, err := consumer.ConsumeRecords(pcm, runtimeState, record, pipelineDBConnector)
	if err != nil {
		_logger.Error("Failed to consume record at destination", zap.Error(err))
		return handleBacklog(pcm, runtimeState, _logger, records, models.FailureStageDestination, err, pipeline, pipelineDBConnector)
	}

	if status == models.RecordPushCommitted {
		if reader.CommitHook != nil {
			if action := reader.CommitHook(records); action != models.ActionContinue {
				return models.ActionStop
			}
		}
		return handleCheckpoint(pcm, runtimeState, _logger, records, pipeline, pipelineDBConnector)
	}

	return models.ActionContinue
}

func handleBacklog(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, records []*models.Record, failureStage models.FailureStage, cause error, pipeline *pipelineContext, pipelineDBConnector *models.PipelineDBConnectors) models.PipelineAction {
	if pipeline.backlogFunc == nil {
		return models.ActionContinue
	}

	backlogParam := models.BacklogProps{
		State:              runtimeState,
		FailureStage:       failureStage,
		Err:                cause,
		Records:            extractRecordData(records),
		SourceDBConn:       pipelineDBConnector.Source,
		DestDBConn:         pipelineDBConnector.Destination,
		AuxiliaryDBConnMap: pipelineDBConnector.AuxilaryHub,
	}

	backlogResponse, err := pipeline.backlogFunc(&backlogParam)
	if err != nil {
		_logger.Error("Backlog handler failed", zap.Error(err))
		if pipelineDBConnector.Destination.IsConnectionError(err) {
			pcm.NotifyConnectionError(err)
			return models.ActionStop
		}
		return backlogResponse.Action
	}

	if backlogResponse != nil && backlogResponse.Action == models.ActionStop {
		_logger.Debug("backlog handler requested pipeline stop")
	}

	return backlogResponse.Action
}

func handleCheckpoint(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, records []*models.Record, pipeline *pipelineContext, pipelineDBConnector *models.PipelineDBConnectors) models.PipelineAction {
	if pipeline.checkpointFunc == nil {
		return models.ActionContinue
	}

	checkpointParam := models.CheckpointProps{
		State:              runtimeState,
		Records:            extractRecordData(records),
		SourceDBConn:       pipelineDBConnector.Source,
		DestDBConn:         pipelineDBConnector.Destination,
		AuxiliaryDBConnMap: pipelineDBConnector.AuxilaryHub,
	}

	checkpointResponse, err := pipeline.checkpointFunc(&checkpointParam)
	if err != nil {
		_logger.Error("Checkpoint handler failed", zap.Error(err))
		if pipelineDBConnector.Source.IsConnectionError(err) {
			pcm.NotifyConnectionError(err)
			return models.ActionStop
		}
		return checkpointResponse.Action
	}

	if checkpointResponse != nil && checkpointResponse.Action == models.ActionStop {
		_logger.Debug("checkpoint handler requested pipeline stop")
	}

	return checkpointResponse.Action
}

func flushPipeline(pcm *contexts.PipelineContextManager, runtimeState *contexts.PipelineRuntimeState, _logger logger.PipelineLogger, reader *models.IReadByImpl, consumer destination.IDestinationRecordsConsumer, pipeline *pipelineContext, pipelineDBConnector *models.PipelineDBConnectors) {
	status, records, err := consumer.Flush(pcm)

	if err != nil {
		_logger.Error("Destination flush failed", zap.Error(err))
		handleBacklog(pcm, runtimeState, _logger, records, models.FailureStageDestination, err, pipeline, pipelineDBConnector)
	}

	if status == models.RecordPushCommitted {
		if reader.CommitHook != nil {
			reader.CommitHook(records)
		}
		handleCheckpoint(pcm, runtimeState, _logger, records, pipeline, pipelineDBConnector)
	}
}

func extractRecordData(records []*models.Record) []map[string]any {
	dataRecords := make([]map[string]any, len(records))
	for i, record := range records {
		dataRecords[i] = record.Data
	}
	return dataRecords
}