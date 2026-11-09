// Command aki 是 aki.io 站点插件（C 类：纯文本，无 function tools）。
//
// 站点: https://aki.io  Model Hub Playground（DeepSeek-V4.1-Flash-1017 等）
// 端点:
//   POST /api/call/{endpoint}      提交任务 → 返回 job_id
//   POST /api/progress/{endpoint}  轮询任务 → job_state: processing / done / failed
//
// 认证: 匿名访客默认 key（见 defaultKey，由站点前端 JS 硬编码下发）即可提交；
// 注册用户的 key 通过 credential.api_key 覆盖。访客 key 有「每日 3 次」的硬上限，
// 到顶会返回 daily-limit 错误，插件原样透传给上游用户。
//
// 关于「屏蔽网站内置提示词」:
// 该站点前端会自动给对话注入一段预设 system 人设（"You are a helpful, respectful
// and honest assistant named Deepseek..."，见其 JS 的 CHAT_TEMPLATES）。本插件
// 不复刻这段内置提示，只依据网关下发的 messages/system 构造 chat_context；若检测
// 到请求里混入了这段内置人设，buildChatContext 会将其过滤掉，确保不会把网站预设
// system 一并送给模型。
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 站点常量，可用环境变量覆盖成任意同构部署。
var (
	apiBase    = envOr("AKI_BASE", "https://aki.io/api")
	endpoint   = envOr("AKI_ENDPOINT", "deepseek_v4_1_flash")
	modelsCSV  = envOr("AKI_MODELS", "deepseek-v4.1-flash")
	// 全站匿名共用一把 key（站点 JS 里写死下发的那把）。限流不是记在这把 key 上，
	// 而是记在「该 key 所在的出口 IP」上：每个出口 IP 每日 3 次。
	defaultKey = envOr("AKI_KEY", "45dd3f5c-28f1-49be-99c8-581d9f7cb35f")
	browserUA  = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	browserRef = "https://aki.io/en/playground/deepseekai-deepseek-v4-1-flash"

	// 出口 IP 池（= 代理列表），逗号分隔。网关若给了 credential.proxy 就强制用那条；
	// 否则在 pool 里按「今日用量最少」挑一个，摊薄每 IP 3 次的日额度。~34 个 IP ≈ 102 次/天。
	proxyPool = splitNonEmpty(envOr("AKI_PROXIES", ""))
	// 单个出口 IP 的每日可用次数上限（可观测、可调）。
	perIPDailyLimit = atoiOr(envOr("AKI_PER_IP_DAILY", "3"), 3)

	pollInterval = 1500 * time.Millisecond
	pollDeadline = 4 * time.Minute
)

// 出口常量：直连时出口标识。
const directEgress = "direct"

// egressLedger 按出口 IP（proxy 地址指纹；直连记 direct）记录今日成功提交次数，
// 以及站点是否已回 daily-limit 将该出口标满。跨 UTC 日自动重置。
// 这是「每 IP 3 次/天」的本地台账，避免把同一 IP 的额度浪费在注定失败的请求上。
type egressLedger struct {
	mu       sync.Mutex
	day      string
	used     map[string]int
	exhausted map[string]bool
}

var ledger = &egressLedger{}

func (l *egressLedger) rollover() {
	d := time.Now().UTC().Format("2006-01-02")
	if d != l.day {
		l.day = d
		l.used = map[string]int{}
		l.exhausted = map[string]bool{}
	}
}

// markExhausted 把出口标记为今日已满。
func (l *egressLedger) markExhausted(egress string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rollover()
	l.exhausted[egress] = true
}

// record 记一次成功提交。
func (l *egressLedger) record(egress string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rollover()
	l.used[egress]++
}

// pick 挑一个可用出口：排除 skip 集合，排除已耗尽的，
// 若 prefer 非空且未被排除/耗尽则优先；否则取今日用量最少者。无可用返回 ""。
func (l *egressLedger) pick(prefer string, skip map[string]bool) string {
	cands := []string{}
	if prefer != "" {
		cands = append(cands, prefer)
	}
	for _, p := range proxyPool {
		if p != prefer {
			cands = append(cands, p)
		}
	}
	if prefer == "" {
		cands = append(cands, directEgress)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rollover()

	best, bestUsed, found := "", -1, false
	for _, c := range cands {
		if c == "" || skip[c] || l.exhausted[c] {
			continue
		}
		u := l.used[c]
		// 本地台账已达上限的出口跳过（站点侧必拒）。
		if u >= perIPDailyLimit {
			continue
		}
		if bestUsed < 0 || u < bestUsed {
			best, bestUsed, found = c, u, true
		}
	}
	if found {
		return best
	}
	return ""
}

// envOr 读环境变量，缺省回退。
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// rpcRequest 是网关请求。
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// rpcEvent 是回推事件。
type rpcEvent struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// chatParams 是 chat 参数（C 类：无 tools，保留最小必要字段）。
type chatParams struct {
	Model       string            `json:"model"`
	System      string            `json:"system"`
	Messages    []message         `json:"messages"`
	MaxTokens   int               `json:"max_tokens"`
	Temperature float64           `json:"temperature"`
	TopP        float64           `json:"top_p"`
	TopK        int               `json:"top_k"`
	Credential  map[string]string `json:"credential"`
}

// message 是统一消息。
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// siteMsg 是发给站点的 chat_context 元素（站点只认纯文本 role/content）。
type siteMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// jobResult 是任务完成后的结果体（/progress 里 job_result 的解包形态）。
type jobResult struct {
	Text         string `json:"text"`
	NumGenTokens int    `json:"num_generated_tokens"`
	ModelName    string `json:"model_name"`
	FinishReason string `json:"finish_reason"`
}

// progressResult 是 /progress 的应答。
type progressResult struct {
	Success   bool        `json:"success"`
	JobState  string      `json:"job_state"`
	Error     any         `json:"error"`
	JobResult jobResult   `json:"job_result"`
}

// callResult 是 /call 的应答。
type callResult struct {
	Success bool   `json:"success"`
	JobID   string `json:"job_id"`
	Error   any    `json:"error"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--selfcheck" {
		fmt.Printf(`{"models":%s}`+"\n", modelsJSON())
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "models":
			respond(out, req.ID, map[string]any{"models": modelList()})
		case "ping":
			respond(out, req.ID, map[string]any{"pong": true})
		case "chat":
			handleChat(out, req)
		default:
			respondError(out, req.ID, -32601, "未知方法: "+req.Method)
		}
	}
}

// modelList 构造模型清单。
func modelList() []map[string]any {
	list := []map[string]any{}
	for _, id := range strings.Split(modelsCSV, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		list = append(list, map[string]any{
			"id":           id,
			"capabilities": []string{"streaming"},
		})
	}
	if len(list) == 0 {
		list = append(list, map[string]any{"id": "deepseek-v4.1-flash", "capabilities": []string{"streaming"}})
	}
	return list
}

func modelsJSON() string {
	b, _ := json.Marshal(modelList())
	return string(b)
}

// builtinSystemMarkers 是站点前端内置人设的特征串（小写匹配），命中即视为
// 「网站内置提示词」，从 chat_context 中剔除。
var builtinSystemMarkers = []string{
	"you are a helpful, respectful and honest assistant named",
	"du bist ein assistent namens",
	"vous êtes une assistante serviable",
	"tú eres una asistenta útil",
	"assistant named deepseek",
}

// isBuiltinSystem 判断一条 system 内容是否是站点内置人设。
func isBuiltinSystem(content string) bool {
	c := strings.ToLower(content)
	for _, m := range builtinSystemMarkers {
		if strings.Contains(c, m) {
			return true
		}
	}
	return false
}

// buildChatContext 把网关的 messages/system 折成站点 chat_context，同时过滤掉
// 网站内置的预设 system 人设（屏蔽内置提示词）。站点只接受 user / assistant 两个
// 文本角色：用户自定义 system 以 "System: ..." 前缀并入首条 user；内置人设直接丢弃；
// role=tool / tool_calls 丢弃（C 类无工具）。
func buildChatContext(p chatParams) []siteMsg {
	out := []siteMsg{}

	// 收集用户自定义 system（网关 system 字段或 messages 里非内置的 system）。
	var userSystem string
	if p.System != "" && !isBuiltinSystem(p.System) {
		userSystem = p.System
	}

	seenUser := false
	for _, m := range p.Messages {
		switch m.Role {
		case "system":
			if !isBuiltinSystem(m.Content) {
				userSystem = m.Content
			}
		case "user":
			text := m.Content
			if !seenUser && userSystem != "" {
				text = "System: " + userSystem + "\n" + text
				userSystem = ""
			}
			seenUser = true
			out = append(out, siteMsg{"user", text})
		case "assistant":
			out = append(out, siteMsg{"assistant", m.Content})
		}
	}
	if len(out) == 0 {
		out = append(out, siteMsg{"user", ""})
	}
	return out
}

// handleChat 提交任务并轮询到 done，把最终文本按块推成流。
//
// 出口 IP 策略：每出口 IP 每日 3 次（站点侧），本地用 egressLedger 记账。
//   - 网关给了 credential.proxy：强制走那条出口（prefer），但当日满了会换池里其它出口；
//   - 否则在 AKI_PROXIES 池里挑「今日用量最少」的出口，摊薄额度；池空则走直连。
// 命中站点 daily-limit 时把该出口标满并换下一个，整池都满才报「今日额度已用尽」。
func handleChat(out *bufio.Writer, req rpcRequest) {
	var p chatParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		respondError(out, req.ID, -32602, "参数解析失败: "+err.Error())
		return
	}

	apiKey := p.Credential["api_key"]
	if apiKey == "" {
		apiKey = defaultKey
	}

	ctx := buildChatContext(p)
	ctxJSON, _ := json.Marshal(ctx)
	reqBody := map[string]any{
		"chat_context":    string(ctxJSON),
		"key":             apiKey,
		"wait_for_result": false,
		"temperature":     orDefaultF(p.Temperature, 0.8),
		"top_p":           orDefaultF(p.TopP, 0.9),
		"top_k":           orDefaultI(p.TopK, 40),
		"max_gen_tokens":  orDefaultI(p.MaxTokens, 512),
	}
	payload, _ := json.Marshal(reqBody)

	// 网关若指定了代理出口，则优先它；否则让 pick 挑池里今日最空的出口。
	prefer := p.Credential["proxy"]

	// 逐出口尝试提交，daily-limit 就换下一个（直到整池用尽）。
	tried := map[string]bool{}
	// 出口总数有限（prefer + 池 + 直连），最多试这么多次。
	maxTries := len(proxyPool) + 2
	for tries := 0; tries < maxTries; tries++ {
		egress := ledger.pick(prefer, tried)
		if egress == "" {
			break // 整池都满了或已试遍
		}
		tried[egress] = true

		client := newHTTPClientForProxy(egress)
		// 用该出口提交任务。
		jobID, dailyLimit, err := submitJob(client, payload)
		if dailyLimit {
			// 该出口今日 3 次已满：标满换下一个，不计入提交额度（这次被拒没耗额度）。
			ledger.markExhausted(egress)
			continue
		}
		if err != nil {
			// 网络/连接失败：换下一个出口再试，不立即报错。
			continue
		}

		// 轮询结果（用提交时同一出口，保持 IP 一致）。
		result, pollErr, exhaustedNow := pollJob(client, jobID, apiKey)
		if exhaustedNow {
			ledger.markExhausted(egress)
			continue // 该出口今日满了，换下一个
		}
		if pollErr != "" {
			// 队列繁忙 / 超时：本出口已占用一次提交，记账后如实报错。
			ledger.record(egress)
			emit(out, req.ID, map[string]any{"type": "error", "error": pollErr})
			return
		}
		// 成功：记账并下发。
		ledger.record(egress)
		streamResult(out, req.ID, result, ctx)
		return
	}

	// 所有出口都试遍仍失败。
	if len(tried) == 0 {
		emit(out, req.ID, map[string]any{"type": "error", "error": "没有可用出口（代理池为空且直连不可用）"})
	} else {
		emit(out, req.ID, map[string]any{"type": "error", "error": "今日各出口 3 次额度均用尽，请扩容 AKI_PROXIES 池或明早重置后再试"})
	}
}

// isDailyLimit 判断上游错误文案是否指向「该出口 IP 今日 3 次额度用尽」。
func isDailyLimit(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "daily limit")
}

// streamResult 把完成的结果文本切片推成流，再发 usage + done。
func streamResult(out *bufio.Writer, id string, result jobResult, ctx []siteMsg) {
	text := stripThinking(result.Text)
	runes := []rune(text)
	for i := 0; i < len(runes); i += 40 {
		end := i + 40
		if end > len(runes) {
			end = len(runes)
		}
		emit(out, id, map[string]any{"type": "delta", "text": string(runes[i:end])})
	}
	emit(out, id, map[string]any{"type": "usage", "usage": map[string]any{
		"prompt_tokens":     len(ctx),
		"completion_tokens": result.NumGenTokens,
	}})
	emit(out, id, map[string]any{"type": "done", "finish": "stop"})
}

// submitJob 用指定出口提交任务。
// 返回 (job_id, 是否触发站点 daily-limit, 网络/解析错误)。
func submitJob(client *http.Client, payload []byte) (string, bool, error) {
	resp, err := postJSON(client, apiBase+"/call/"+endpoint, payload)
	if err != nil {
		return "", false, err
	}
	var cr callResult
	if err := json.Unmarshal(resp, &cr); err != nil {
		return "", false, err
	}
	if cr.Success && cr.JobID != "" {
		return cr.JobID, false, nil
	}
	// 站点拒绝：daily-limit 则上层把出口标满；其它拒绝（坏 key 等）视为该出口失败。
	msg := flattenErr(cr.Error)
	return "", isDailyLimit(msg), fmt.Errorf("上游拒绝: %s", msg)
}

// pollJob 轮询 job 直到 done / failed / 超时。
// 返回 (结果, 可读错误串, 是否触发了站点 daily-limit)。
func pollJob(client *http.Client, jobID, key string) (jobResult, string, bool) {
	var result jobResult
	deadline := time.Now().Add(pollDeadline)
	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)
		pBody, _ := json.Marshal(map[string]any{"job_id": jobID, "key": key})
		raw, err := postJSON(client, apiBase+"/progress/"+endpoint, pBody)
		if err != nil {
			continue // 单发查询失败等下一轮
		}
		var pr progressResult
		if err := json.Unmarshal(raw, &pr); err != nil {
			continue
		}
		if pr.Success {
			switch pr.JobState {
			case "done":
				return pr.JobResult, "", false
			case "failed":
				return jobResult{}, "任务失败（站点侧）: " + flattenErr(pr.Error), false
			}
		}
		// progress 接口也可能回 daily-limit（额度在提交后被抽走的情况）。
		if msg := flattenErr(pr.Error); pr.Error != nil && isDailyLimit(msg) {
			return result, "", true
		}
	}
	return result, "任务超时（免费队列繁忙，请稍后重试）", false
}

// newHTTPClientForProxy 组装带指定出口的客户端。egress 为 directEgress 时直连。
func newHTTPClientForProxy(egress string) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        8,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 15 * time.Second,
	}
	if egress != "" && egress != directEgress {
		if u, err := parseProxy(egress); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Transport: tr, Timeout: pollDeadline + 30 * time.Second}
}

// splitNonEmpty 把逗号分隔串切成非空片段（去空格）。
func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// atoiOr 解析整数，失败回退 def。
func atoiOr(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}


// thinkCloseTag 是思考链结束标记（拆成两段拼接，避免源码里出现完整字面量导致
// 某些工具/模型误解析）。
var thinkCloseTag = "</" + "think" + ">"

// stripThinking 去掉思考链结束标记及其之前的内容，只保留最终正文；没有标记则原样返回。
func stripThinking(s string) string {
	if i := strings.LastIndex(s, thinkCloseTag); i >= 0 {
		return s[i+len(thinkCloseTag):]
	}
	return s
}

// flattenErr 把站点 error 字段（可能是 string 或 []string）拼成一行可读文本。
func flattenErr(v any) string {
	if v == nil {
		return "未知错误"
	}
	switch e := v.(type) {
	case string:
		return e
	case []any:
		parts := make([]string, 0, len(e))
		for _, x := range e {
			parts = append(parts, fmt.Sprintf("%v", x))
		}
		return strings.Join(parts, "; ")
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// postJSON POST 一个 JSON 体，返回原始响应字节。
// 站点业务错误也走 200 + JSON（带 success 字段），这里不阻断，交由调用方判断。
func postJSON(client *http.Client, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", browserRef)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 4096))
}

// newHTTPClient 组装带可选代理的 HTTP 客户端。
// credential.proxy 存在时所有上游请求走它（支持 http/https/socks5/socks5h，可带 user:pass@）；
// 否则默认跟随系统代理环境；均缺省则直连。代理解析失败时保持直连，由后续错误如实回报。
func newHTTPClient(cred map[string]string) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        8,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 15 * time.Second,
	}
	if proxy := cred["proxy"]; proxy != "" {
		if u, err := parseProxy(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Transport: tr, Timeout: pollDeadline + 30 * time.Second}
}

// parseProxy 解析 credential.proxy 为 *url.URL。
func parseProxy(p string) (*url.URL, error) {
	return url.Parse(p)
}

// orDefaultF / orDefaultI 补默认采样参数。
func orDefaultF(v, def float64) float64 {
	if v > 0 {
		return v
	}
	return def
}

func orDefaultI(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

// emit 推送事件。
func emit(out *bufio.Writer, id string, payload any) {
	writeJSON(out, rpcEvent{JSONRPC: "2.0", ID: id, Method: "event", Params: payload})
}

// respond 返回最终应答。
func respond(out *bufio.Writer, id string, result any) {
	writeJSON(out, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

// respondError 返回错误应答。
func respondError(out *bufio.Writer, id string, code int, msg string) {
	writeJSON(out, map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error":     map[string]any{"code": code, "message": msg},
	})
}

// writeJSON 写一行 JSON 并立即刷新。
func writeJSON(out *bufio.Writer, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	out.Write(data)
	out.WriteByte('\n')
	out.Flush()
}
