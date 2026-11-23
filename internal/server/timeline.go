package server

import (
	"bufio"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
)

// 时间线读的是事件日志，不是另一份为界面准备的表（OBS-062）。
//
// 总线的内存环在重启后是空的。日志把同一条事件留在磁盘上，
// 时间线只是按设备把这份日志翻出来。

const eventLogName = "events.jsonl"

const (
	eventLogSyncInterval = 100 * time.Millisecond
	eventLogSyncBatch    = 256
)

func (s *Server) watchEvents(ctxDone <-chan struct{}) {
	defer s.wg.Done()
	path := filepath.Join(s.dd.Root(), eventLogName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.log.Error("事件日志无法打开", "错误", err.Error())
		return
	}
	w := bufio.NewWriterSize(f, 64<<10)
	enc := json.NewEncoder(w)
	pending := 0
	flush := func() bool {
		if pending == 0 {
			return true
		}
		if err := w.Flush(); err != nil {
			s.log.Error("事件日志刷新失败", "错误", err.Error())
			return false
		}
		if err := f.Sync(); err != nil {
			s.log.Error("事件日志同步失败", "错误", err.Error())
			return false
		}
		pending = 0
		if st, err := f.Stat(); err == nil && st.Size() > 8<<20 {
			_ = f.Close()
			_ = os.Rename(path, path+".1")
			f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				s.log.Error("事件日志无法轮转", "错误", err.Error())
				return false
			}
			w = bufio.NewWriterSize(f, 64<<10)
			enc = json.NewEncoder(w)
		}
		return true
	}
	defer func() {
		_ = flush()
		_ = f.Close()
	}()
	ticker := time.NewTicker(eventLogSyncInterval)
	defer ticker.Stop()

	ch, cancel := s.bus.Subscribe(events.Filter{
		Kinds: events.KindConn.Mask() | events.KindSubscription.Mask() |
			events.KindAuthz.Mask() | events.KindMeta.Mask() | events.KindQuota.Mask(),
	}, 1024)
	defer cancel()

	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			if !flush() {
				return
			}
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(toSSE(&ev)); err != nil {
				s.log.Error("事件日志写入失败", "错误", err.Error())
				return
			}
			pending++
			if pending < eventLogSyncBatch {
				continue
			}
			if !flush() {
				return
			}
		}
	}
}

func (a *adminServer) handleTimeline(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var before uint64
	raw := r.URL.Query().Get("beforeSeq")
	if raw == "" {
		raw = r.URL.Query().Get("before")
	}
	if raw != "" {
		before, _ = strconv.ParseUint(raw, 10, 64)
	}
	rows := readTimeline(a.s.dd.Root(), name, before, limit)
	if rows == nil {
		rows = []sseEvent{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": rows})
}

func readTimeline(dir, thing string, before uint64, limit int) []sseEvent {
	var matched []sseEvent
	for _, name := range []string{eventLogName + ".1", eventLogName} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			var ev sseEvent
			if json.Unmarshal(sc.Bytes(), &ev) != nil {
				continue
			}
			if ev.Thing != thing {
				continue
			}
			if before != 0 && ev.Seq >= before {
				continue
			}
			matched = append(matched, ev)
		}
		_ = f.Close()
	}
	if len(matched) > limit {
		matched = matched[len(matched)-limit:]
	}
	for i, j := 0, len(matched)-1; i < j; i, j = i+1, j-1 {
		matched[i], matched[j] = matched[j], matched[i]
	}
	return matched
}
