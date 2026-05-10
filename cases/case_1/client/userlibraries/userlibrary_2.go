package client_userlibrary

import (
	"etlfunnel/execution/models"
	"fmt"
	"strings"
)

// ParseCompany extracts the company prefix from a flow name.
// Flow name format: "{company}_{zone}"  e.g. "vodafone_north" → "vodafone"
func ParseCompany(flowName string) string {
	return strings.SplitN(flowName, "_", 2)[0]
}

// ParseZoneFromTable extracts the zone segment from a replica table name.
// Table format: "{entityBaseName}_{zone}_{splitIndex}" e.g. "customers_maharashtra_1".
// The zone may itself contain underscores (e.g. "andhra_pradesh").
func ParseZoneFromTable(table, entityBaseName string) (string, error) {
	prefix := entityBaseName + "_"
	if !strings.HasPrefix(table, prefix) {
		return "", fmt.Errorf("table %q does not start with entityBaseName prefix %q", table, entityBaseName)
	}
	rest := strings.TrimPrefix(table, prefix)
	parts := strings.Split(rest, "_")
	if len(parts) < 2 {
		return "", fmt.Errorf("table %q: cannot extract zone from suffix %q", table, rest)
	}
	// Last part is the numeric split index; everything before it is the zone.
	return strings.Join(parts[:len(parts)-1], "_"), nil
}

// ParseZone extracts the zone suffix from a flow name.
// Flow name format: "{company}_{zone}"  e.g. "idea_south" → "south"
func ParseZone(flowName string) (string, error) {
	parts := strings.Split(flowName, "_")
	if len(parts) < 2 {
		return "", fmt.Errorf("unexpected flow name %q: expected {company}_{zone}", flowName)
	}
	zone := parts[len(parts)-1]
	if zone == "" {
		return "", fmt.Errorf("could not derive zone from flow=%q", flowName)
	}
	return zone, nil
}

// ParseZoneState extracts zone from the flow name and state from the pipeline name.
// Flow name format: "{company}_{zone}", Pipeline name: "{state}"
func ParseZoneState(flowName, pipelineName string) (zone, state string, err error) {
	parts := strings.Split(flowName, "_")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unexpected flow name %q: expected {company}_{zone}", flowName)
	}
	zone = parts[len(parts)-1]
	state = pipelineName
	if zone == "" || state == "" {
		return "", "", fmt.Errorf("could not derive zone/state from flow=%q pipeline=%q", flowName, pipelineName)
	}
	return zone, state, nil
}

// ParseShardContext derives (company, zone, pipelineState) from pipeline runtime state.
// Flow name format: "{company}_{zone}", Pipeline name: "{state}"
func ParseShardContext(runtimeState models.IPipelineRuntimeState) (company, zone, pipelineState string) {
	flowName := runtimeState.GetFlowName()
	parts := strings.SplitN(flowName, "_", 2)
	if len(parts) == 2 {
		company = parts[0]
		zone = parts[1]
	} else {
		company = flowName
		zone = "unknown"
	}
	pipelineState = runtimeState.GetName()
	return
}
