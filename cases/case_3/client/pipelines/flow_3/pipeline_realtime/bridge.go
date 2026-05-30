package client_pipeline_realtime

// Realtime Pulse Pipeline — flow_3
//
// Polls the GA4 Realtime API (runRealtimeReport) every 60 seconds for all
// three properties and streams rows directly into dbo.realtime_sessions
// (no staging table, no MERGE).  After each INSERT batch the pipeline issues
// a TTL DELETE removing rows older than 2 hours.
//
// This pipeline uses connector_45's StreamRecords path rather than
// GeneratePaginateRequest.  The source engine reads from the channel returned
// by StreamRecords and delivers records to the bridge via reader.Channel.
//
// Transformer chain (subset — no DateParser or DimensionNormaliser needed):
//   transformer_3  SurfaceInjector
//   transformer_4  PropertyInjector
//   transformer_5  NullFiller
//   transformer_8  RunIDStamper

import (
	"fmt"
	"sync/atomic"
	"time"

	client_source_entity      "etlfunnel/execution/client/connectors/connector_45/iso_entity_124"
	client_destination_entity "etlfunnel/execution/client/connectors/connector_47/iso_entity_126"
	client_backlog_1          "etlfunnel/execution/client/backlogs/backlog_1"
	client_terminate_1        "etlfunnel/execution/client/terminates/terminate_1"
	client_transformer_3      "etlfunnel/execution/client/transformers/transformer_3"
	client_transformer_4      "etlfunnel/execution/client/transformers/transformer_4"
	client_transformer_5      "etlfunnel/execution/client/transformers/transformer_5"
	client_transformer_8      "etlfunnel/execution/client/transformers/transformer_8"
	"etlfunnel/execution/contexts"
	"etlfunnel/execution/core/destination"
	"etlfunnel/execution/core/source"
	"etlfunnel/execution/global"
	"etlfunnel/execution/logger"
	"etlfunnel/execution/models"

	"go.uber.org/zap"
)

type pipelineContext struct {
	pcm                     *contexts.PipelineContextManager
	runtimeState            *contexts.PipelineRuntimeState
	log                     logger.PipelineLogger
	dbConnector             *models.PipelineDBConnectors
	clientSourceBridge      *client_source_entity.IUseConnector
	clientDestinationBridge *client_destination_entity.IUseConnector
	backlogFunc             func(*models.BacklogProps) (*models.BacklogTune, error)
	transformerParam        models.TransformerProps
}

func PerformOperations(
	pcm *contexts.PipelineContextManager,
	producer source.ISourceRecordsProducer,
	consumer destination.IDestinationRecordsConsumer,
	pipelineDBConnector *models.PipelineDBConnectors,
) {
	_logger := logger.Pipeline(pcm.GetContext())
	runtimeState := contexts.NewPipelineRuntimeState(pcm, _logger)
	_logger.Debug("realtime pipeline starting")

	pipeline, err := initializePipeline(pcm, runtimeState, _logger, producer, consumer, pipelineDBConnector)
	if err != nil {
		_logger.Error("pipeline initialization failed", zap.Error(err))
		return
	}

	if err := runtimeState.RegisterTermination(client_terminate_1.TerminateRule); err != nil {
		_logger.Error("failed to register termination rule", zap.Error(err))
		return
	}

	reader, err := producer.ProduceRecords(&source.ExecutorContext{
		PCM:                 pcm,
		State:               pipeline.runtimeState,
		PipelineDBConnector: pipelineDBConnector,
		CaptureMethod:       models.CaptureMethodStream,
	})
	if err != nil {
		_logger.Error("source producer failed", zap.Error(err))
		return
	}
	if reader == nil {
		_logger.Error("source producer returned nil reader")
		return
	}

	processRecords(pipeline, reader, consumer)
	flushPipeline(pipeline, reader, consumer)

	if reader.Cleanup != nil {
		if err := reader.Cleanup(); err != nil {
			_logger.Error("source cleanup failed", zap.Error(err))
		}
	}

	_logger.Debug("realtime pipeline stopped")
}

func initializePipeline(
	pcm *contexts.PipelineContextManager,
	runtimeState *contexts.PipelineRuntimeState,
	_logger logger.PipelineLogger,
	producer source.ISourceRecordsProducer,
	consumer destination.IDestinationRecordsConsumer,
	pipelineDBConnector *models.PipelineDBConnectors,
) (*pipelineContext, error) {
	pipeline := &pipelineContext{
		pcm:                     pcm,
		runtimeState:            runtimeState,
		log:                     _logger,
		dbConnector:             pipelineDBConnector,
		clientSourceBridge:      &client_source_entity.IUseConnector{},
		clientDestinationBridge: &client_destination_entity.IUseConnector{},
	}
	pipeline.backlogFunc = client_backlog_1.Backlog

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

func processRecords(
	pipeline *pipelineContext,
	reader *models.IReadByImpl,
	consumer destination.IDestinationRecordsConsumer,
) {
	var totalMessages atomic.Uint64
	startTime := time.Now()
	var lastMessageAt atomic.Value
	lastMessageAt.Store(startTime)
	defer pipeline.runtimeState.StopControlPlane()

	for {
		select {
		case <-pipeline.pcm.GetContext().Done():
			return

		case <-pipeline.runtimeState.TerminationChan():
			if should, reason := pipeline.runtimeState.EvaluateTermination(
				totalMessages.Load(), lastMessageAt.Load().(time.Time), startTime,
			); should {
				pipeline.log.Info("termination condition met: " + reason)
				return
			}

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

			transformed, err := applyTransformations(record.Data, pipeline)
			if err != nil {
				pipeline.log.Error("transformation failed", zap.Error(err))
				if action := handleBacklog(pipeline, []*models.Record{record}, models.FailureStageTransform, err); action != models.ActionContinue {
					return
				}
				continue
			}
			if transformed == nil {
				continue
			}
			record.Data = transformed

			if action := consumeRecord(pipeline, consumer, record); action != models.ActionContinue {
				return
			}
		}
	}
}

func applyTransformations(record map[string]any, pipeline *pipelineContext) (map[string]any, error) {
	transformers := []func(*models.TransformerProps) (map[string]any, error){
		client_transformer_3.Transformer,
		client_transformer_4.Transformer,
		client_transformer_5.Transformer,
		client_transformer_8.Transformer,
	}
	result := record
	var err error
	for _, t := range transformers {
		pipeline.transformerParam.Record = result
		result, err = t(&pipeline.transformerParam)
		if err != nil || result == nil {
			return nil, err
		}
	}
	return result, nil
}

func consumeRecord(pipeline *pipelineContext, consumer destination.IDestinationRecordsConsumer, record *models.Record) models.PipelineAction {
	_, records, err := consumer.ConsumeRecords(pipeline.pcm, pipeline.runtimeState, record, pipeline.dbConnector)
	if err != nil {
		pipeline.log.Error("failed to consume record", zap.Error(err))
		return handleBacklog(pipeline, records, models.FailureStageDestination, err)
	}
	return models.ActionContinue
}

func handleBacklog(pipeline *pipelineContext, records []*models.Record, stage models.FailureStage, cause error) models.PipelineAction {
	if pipeline.backlogFunc == nil {
		return models.ActionContinue
	}
	resp, err := pipeline.backlogFunc(&models.BacklogProps{
		State:              pipeline.runtimeState,
		FailureStage:       stage,
		Err:                cause,
		Records:            extractData(records),
		SourceDBConn:       pipeline.dbConnector.Source,
		DestDBConn:         pipeline.dbConnector.Destination,
		AuxiliaryDBConnMap: pipeline.dbConnector.AuxilaryHub,
	})
	if err != nil {
		pipeline.log.Error("backlog handler failed", zap.Error(err))
		if pipeline.dbConnector.Destination.IsConnectionError(err) {
			pipeline.pcm.NotifyConnectionError(err)
			return models.ActionStop
		}
	}
	if resp != nil {
		return resp.Action
	}
	return models.ActionContinue
}

func flushPipeline(pipeline *pipelineContext, reader *models.IReadByImpl, consumer destination.IDestinationRecordsConsumer) {
	_, records, err := consumer.Flush(pipeline.pcm)
	if err != nil {
		pipeline.log.Error("destination flush failed", zap.Error(err))
		handleBacklog(pipeline, records, models.FailureStageDestination, err)
	}
}

func extractData(records []*models.Record) []map[string]any {
	out := make([]map[string]any, len(records))
	for i, r := range records {
		out[i] = r.Data
	}
	return out
}
