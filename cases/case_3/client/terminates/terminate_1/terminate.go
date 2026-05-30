package client_terminate_1

// TerminateRule — defines the runtime stop conditions for all three GA4
// pipeline flows:
//
//   MANUAL_KILL   — operator sets force_stop=true in AuxDB.terminate_rules
//   IDLE_TIMEOUT  — no records received for 5 minutes (source is exhausted)
//
// The rule is evaluated every 10 seconds by the framework's control plane.

import (
	"time"

	"etlfunnel/execution/models"
)

const (
	checkInterval = 10 * time.Second
	idleTimeout   = 5 * time.Minute
)

func TerminateRule(param *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	return &models.TerminateRuleTune{
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: makeCheckFunc(),
	}, nil
}

func makeCheckFunc() func(*models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	return func(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
		// IDLE_TIMEOUT: source has stopped producing records.
		if !props.LastMessageAt.IsZero() {
			if time.Since(props.LastMessageAt) >= idleTimeout {
				return &models.TerminateRuleActionTune{
					Reason: "IDLE_TIMEOUT",
					Action: models.ActionStop,
				}, nil
			}
		}

		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
}
