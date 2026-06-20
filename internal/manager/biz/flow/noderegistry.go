// noderegistry.go — the node-type registry (HLD-016 node abstraction).
//
// A node type is a first-class, self-describing entity: one NodeSpec
// declares its structural behaviour (Kind), palette grouping (Category),
// control ports, and executor. The engine, graph validation, and trigger
// detection all DERIVE from the registry — there is no per-type switch,
// no "trigger." string-prefix convention, no knownTypes map. Adding a node
// type = RegisterNode(one spec); nothing in the core engine changes.
package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// NodeKind classifies a node's structural behaviour so the engine and
// validator never special-case by Type string.
type NodeKind string

const (
	KindTrigger NodeKind = "trigger" // entry point: no inbound edge, no error port
	KindAction  NodeKind = "action"  // calls a capability: agent / llm / tool / notify
	KindControl NodeKind = "control" // branches/merges the flow: condition / (future) merge
	KindData    NodeKind = "data"    // shapes data: set / (future) transform
)

// ExecuteFunc is the universal node contract: resolved config + read-only
// run context → (data output, control port) | error. Stateless — the
// Executors seam bundle is passed in, not captured, so a spec can be
// registered globally.
type ExecuteFunc func(ctx context.Context, x Executors, cfg map[string]any, rc *RunContext) (NodeResult, error)

// NodeSpec is the complete declaration of one node type.
type NodeSpec struct {
	Type     string      // wire type ("tool" / "llm" / "transform")
	Kind     NodeKind    // structural behaviour
	Category string      // palette grouping (intent-based)
	Ports    []string    // control output ports (default [next]; condition=[true,false])
	Execute  ExecuteFunc // executor
}

var nodeRegistry = map[string]*NodeSpec{}

// RegisterNode adds (or replaces) a node type. Ports defaults to [next].
func RegisterNode(s *NodeSpec) {
	if s == nil || s.Type == "" {
		return
	}
	if len(s.Ports) == 0 {
		s.Ports = []string{PortNext}
	}
	nodeRegistry[s.Type] = s
}

// LookupNode returns the spec for a type, or nil if unregistered.
func LookupNode(t string) *NodeSpec { return nodeRegistry[t] }

// AllNodeSpecs returns every registered spec (palette / node-types API).
func AllNodeSpecs() []*NodeSpec {
	out := make([]*NodeSpec, 0, len(nodeRegistry))
	for _, s := range nodeRegistry {
		out = append(out, s)
	}
	return out
}

func init() { registerBuiltins() }

// registerBuiltins wires the built-in node types. New built-ins land here;
// dynamically-discovered types (tools come from BaseTool schemas) keep the
// single `tool` spec and select via config.tool.
func registerBuiltins() {
	RegisterNode(&NodeSpec{Type: NodeTriggerManual, Kind: KindTrigger, Category: "trigger", Execute: execTrigger})
	RegisterNode(&NodeSpec{Type: NodeTriggerAlert, Kind: KindTrigger, Category: "trigger", Execute: execTrigger})
	RegisterNode(&NodeSpec{Type: NodeTriggerCron, Kind: KindTrigger, Category: "trigger", Execute: execTrigger})
	RegisterNode(&NodeSpec{Type: NodeAgent, Kind: KindAction, Category: "ai", Execute: execAgent})
	RegisterNode(&NodeSpec{Type: NodeLLM, Kind: KindAction, Category: "ai", Execute: execLLM})
	RegisterNode(&NodeSpec{Type: NodeTool, Kind: KindAction, Category: "action", Execute: execTool})
	RegisterNode(&NodeSpec{Type: NodeCondition, Kind: KindControl, Category: "flow", Ports: []string{PortTrue, PortFalse}, Execute: execCondition})
	RegisterNode(&NodeSpec{Type: NodeNotify, Kind: KindAction, Category: "action", Execute: execNotify})
	RegisterNode(&NodeSpec{Type: NodeSet, Kind: KindData, Category: "data", Execute: execSet})
	RegisterNode(&NodeSpec{Type: NodeTransform, Kind: KindData, Category: "data", Execute: execTransform})
}

// --- built-in executors (migrated verbatim from the old execute switch) ---

func execTrigger(_ context.Context, _ Executors, _ map[string]any, rc *RunContext) (NodeResult, error) {
	// Every trigger's "output" is the trigger payload itself so downstream
	// can use either {{trigger.x}} or {{nodes.<id>.output.x}}. The payload
	// differs by source (manual input / incident context / cron fire time)
	// but the node behaviour is identical.
	return NodeResult{Output: anyMap(rc.Trigger), Port: PortNext}, nil
}

func execAgent(ctx context.Context, x Executors, cfg map[string]any, _ *RunContext) (NodeResult, error) {
	if x.Agent == nil {
		return NodeResult{}, fmt.Errorf("agent node: runner not wired")
	}
	persona, _ := cfg["persona"].(string)
	if persona == "" {
		persona = "default"
	}
	instruction, _ := cfg["instruction"].(string)
	if strings.TrimSpace(instruction) == "" {
		return NodeResult{}, fmt.Errorf("agent node: instruction is empty")
	}
	// Structured gateway (HLD-016): with an output_schema the agent is
	// instructed to answer ONLY the JSON object; we parse it into
	// output.structured so deterministic downstream (condition / tool args)
	// can reference fields. Without one the answer is free text.
	schema, hasSchema := cfg["output_schema"].(map[string]any)
	prompt := instruction
	if hasSchema {
		sb, _ := json.Marshal(schema)
		prompt = instruction + "\n\nReturn ONLY a single JSON object matching this JSON Schema (no prose, no code fence):\n" + string(sb)
	}
	actx, cancel := context.WithTimeout(ctx, agentNodeTimeout)
	defer cancel()
	answer, err := x.Agent.RunAgent(actx, persona, prompt)
	if err != nil {
		return NodeResult{}, fmt.Errorf("agent node: %w", err)
	}
	out := map[string]any{"answer": answer}
	if hasSchema {
		structured, perr := parseLooseJSON(answer)
		if perr != nil {
			return NodeResult{}, fmt.Errorf("agent node: output_schema declared but answer is not JSON: %w", perr)
		}
		out["structured"] = structured
	}
	return NodeResult{Output: out, Port: PortNext}, nil
}

func execLLM(ctx context.Context, x Executors, cfg map[string]any, _ *RunContext) (NodeResult, error) {
	if x.LLM == nil {
		return NodeResult{}, fmt.Errorf("llm node: runner not wired")
	}
	prompt, _ := cfg["prompt"].(string)
	if strings.TrimSpace(prompt) == "" {
		return NodeResult{}, fmt.Errorf("llm node: prompt is empty")
	}
	system, _ := cfg["system"].(string)
	schema, hasSchema := cfg["output_schema"].(map[string]any)
	if hasSchema {
		sb, _ := json.Marshal(schema)
		prompt = prompt + "\n\nReturn ONLY a single JSON object matching this JSON Schema (no prose, no code fence):\n" + string(sb)
	}
	lctx, cancel := context.WithTimeout(ctx, llmNodeTimeout)
	defer cancel()
	answer, err := x.LLM.RunLLM(lctx, system, prompt)
	if err != nil {
		return NodeResult{}, fmt.Errorf("llm node: %w", err)
	}
	out := map[string]any{"answer": answer}
	if hasSchema {
		structured, perr := parseLooseJSON(answer)
		if perr != nil {
			return NodeResult{}, fmt.Errorf("llm node: output_schema declared but answer is not JSON: %w", perr)
		}
		out["structured"] = structured
	}
	return NodeResult{Output: out, Port: PortNext}, nil
}

func execTool(ctx context.Context, x Executors, cfg map[string]any, _ *RunContext) (NodeResult, error) {
	if x.Tools == nil {
		return NodeResult{}, fmt.Errorf("tool node: invoker not wired")
	}
	name, _ := cfg["tool"].(string)
	if name == "" {
		return NodeResult{}, fmt.Errorf("tool node: missing tool name")
	}
	args, _ := cfg["args"].(map[string]any)
	ab, err := json.Marshal(args)
	if err != nil {
		return NodeResult{}, fmt.Errorf("tool node: args: %w", err)
	}
	tctx, cancel := context.WithTimeout(ctx, defaultNodeTimeout)
	defer cancel()
	res, err := x.Tools.InvokeTool(tctx, name, ab)
	if err != nil {
		return NodeResult{}, fmt.Errorf("tool %s: %w", name, err)
	}
	var out any
	if len(res) > 0 {
		if uerr := json.Unmarshal(res, &out); uerr != nil {
			out = string(res) // non-JSON tool output stays a string
		}
	}
	return NodeResult{Output: map[string]any{"result": out}, Port: PortNext}, nil
}

func execCondition(_ context.Context, _ Executors, cfg map[string]any, rc *RunContext) (NodeResult, error) {
	expr, _ := cfg["expr"].(string)
	if strings.TrimSpace(expr) == "" {
		return NodeResult{}, fmt.Errorf("condition node: missing expr")
	}
	ok, err := rc.EvalCondition(expr)
	if err != nil {
		return NodeResult{}, err
	}
	port := PortFalse
	if ok {
		port = PortTrue
	}
	return NodeResult{Output: map[string]any{"result": ok}, Port: port}, nil
}

func execNotify(ctx context.Context, x Executors, cfg map[string]any, _ *RunContext) (NodeResult, error) {
	if x.Notify == nil {
		return NodeResult{}, fmt.Errorf("notify node: notifier not wired")
	}
	title, _ := cfg["title"].(string)
	message, _ := cfg["message"].(string)
	if strings.TrimSpace(message) == "" {
		return NodeResult{}, fmt.Errorf("notify node: message is empty")
	}
	ids := toUint64s(cfg["channel_ids"])
	if len(ids) == 0 {
		return NodeResult{}, fmt.Errorf("notify node: no channel_ids")
	}
	nctx, cancel := context.WithTimeout(ctx, defaultNodeTimeout)
	defer cancel()
	if err := x.Notify.Notify(nctx, ids, title, message); err != nil {
		return NodeResult{}, fmt.Errorf("notify node: %w", err)
	}
	return NodeResult{Output: map[string]any{"sent": true, "channels": len(ids)}, Port: PortNext}, nil
}

func execSet(_ context.Context, _ Executors, cfg map[string]any, rc *RunContext) (NodeResult, error) {
	name, _ := cfg["name"].(string)
	if name == "" {
		return NodeResult{}, fmt.Errorf("set node: missing var name")
	}
	val := cfg["value"]
	rc.Vars[name] = val
	return NodeResult{Output: map[string]any{"name": name, "value": val}, Port: PortNext}, nil
}

// execTransform — the "edit fields" data node. config.fields is an
// object {outName: value}; the engine has already template-resolved every
// value, so the node just emits that constructed object. Lets a user
// reshape upstream data into the exact fields a downstream node needs
// (the field-adapter glue for a weakly-typed flow), with no code and no
// new dependency — values are plain {{...}} templates.
func execTransform(_ context.Context, _ Executors, cfg map[string]any, _ *RunContext) (NodeResult, error) {
	fields, _ := cfg["fields"].(map[string]any)
	if fields == nil {
		fields = map[string]any{}
	}
	return NodeResult{Output: fields, Port: PortNext}, nil
}
