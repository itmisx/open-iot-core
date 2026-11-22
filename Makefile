# open-iot-core 构建与门禁
#
# PRD docs/prd/19-testing.md 的 TST-070 至 TST-076 定义了合并门禁，
# 本文件是其可执行形式。CI 与本地执行同一套目标。

GO        ?= go
PKGS      := ./...
# 整体覆盖率的分母是**随二进制发布的**包（TST-166）。
#
# tools/ 下的两样东西（契约检查器与压测器）不进二进制：把一个没有行覆盖的
# 开发工具算进分母，等于用门禁的余量去抵一件与产品行为无关的事——压测器
# 一加进来，整体覆盖率从 85.2% 掉到 82.2%，而产品代码一行没改。
# 它们该有的那种验证是另一类：vet、fmt 与「量出来的数对不对」的用例
# （tools/mqttbench/main_test.go 盯的是算错了也没有现象的那几处）。
COVER_PKGS := ./cmd/... ./internal/...
MQTT_PKGS  := ./internal/mqtt/...
# TST-010 要求 95% 覆盖率的包：协议层与策略层
# 严格档：语义保真度直接决定兼容性的包。
# shadow 在列是因为 PRD SHDW-021 明说元数据语义是最易做错的一处，
# 而做错的表现是文档对设备撒谎——没有任何操作会报错。
STRICT_PKGS := ./internal/mqtt/... ./internal/authz/... ./internal/shadow/...
# fuzz 的 -fuzz 标志只接受单个包，故按包逐一列出
PACKET_PKG := ./internal/mqtt/packet/
TOPIC_PKG  := ./internal/mqtt/topic/
AUTHZ_PKG  := ./internal/authz/
SHADOW_PKG := ./internal/shadow
BENCH_PKGS := $(PACKET_PKG) $(TOPIC_PKG) $(AUTHZ_PKG) ./internal/router/ ./internal/meta/
FUZZTIME  ?= 30s
# demo 的产物目录。与 scripts/demo.sh 的同名变量是同一个旋钮，
# export 是必要的：Makefile 里 ?= 定义的变量不会自动进到 recipe 的环境里，
# 不 export 则 `make demo DEMO_DIR=x` 只改了 clean 这一半，脚本仍用 .demo。
DEMO_DIR  ?= .demo
export DEMO_DIR
COVERFILE := coverage.out

.PHONY: all
all: gate

# ---------- 门禁：合并前必须全绿 ----------
.PHONY: gate
gate: fmt-check vet contract test-race cover-check cross fuzz-short

.PHONY: fmt-check
fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "格式不规范:"; echo "$$out"; exit 1; fi
	@echo "fmt: 通过"

# 可选的本地整理，使用比 gofmt 更严的 gofumpt。不进门禁，
# 以免 CI 依赖网络下载与工具链切换。
.PHONY: fmt
fmt:
	@$(GO) run mvdan.cc/gofumpt@v0.7.0 -w .

.PHONY: vet
vet:
	@$(GO) vet $(PKGS)
	@echo "vet: 通过"
# ARCH-052 至 ARCH-058：架构契约的机械检查。
# 规则表在 tools/contractcheck/main.go，新增契约时在那里登记。
.PHONY: contract
contract:
	@$(GO) run ./tools/contractcheck

.PHONY: test
test:
	@$(GO) test $(PKGS)

# TST-074：竞态检测下全部测试通过
.PHONY: test-race
test-race:
	@$(GO) test -race $(PKGS)
	@echo "race: 通过"

.PHONY: cover
cover:
	@$(GO) test -coverprofile=$(COVERFILE) -coverpkg=$(PKGS) $(PKGS)
	@$(GO) tool cover -func=$(COVERFILE) | tail -1

# TST-010：协议层与策略层不低于 95%，整体不低于 85%
.PHONY: cover-check
cover-check:
	@for pkg in $(STRICT_PKGS); do \
		$(GO) test -coverprofile=$(COVERFILE).strict -coverpkg=$$pkg $$pkg >/dev/null || exit 1; \
		pct=$$($(GO) tool cover -func=$(COVERFILE).strict | tail -1 | awk '{gsub("%","",$$3); print $$3}'); \
		printf "%-24s 覆盖率: %s%%（门槛 95%%）\n" "$$pkg" "$$pct"; \
		awk -v p=$$pct -v n=$$pkg 'BEGIN { if (p+0 < 95) { print n" 低于门槛 95%"; exit 1 } }' || exit 1; \
	done
	@$(GO) test -coverprofile=$(COVERFILE) \
		-coverpkg=$$(echo $(COVER_PKGS) | tr ' ' ,) $(COVER_PKGS) >/dev/null
	@pct=$$($(GO) tool cover -func=$(COVERFILE) | tail -1 | awk '{gsub("%","",$$3); print $$3}'); \
	echo "整体覆盖率:        $$pct%（门槛 85%）"; \
	awk -v p=$$pct 'BEGIN { if (p+0 < 85) { print "低于门槛 85%"; exit 1 } }'
	@echo "cover: 通过"

.PHONY: cover-html
cover-html: cover
	@$(GO) tool cover -html=$(COVERFILE) -o coverage.html
	@echo "已生成 coverage.html"

# TST-073：三平台交叉编译，且不依赖 CGO（NFR-050）
.PHONY: cross
cross:
	@for t in linux/amd64 linux/arm64 darwin/arm64; do \
		os=$${t%/*}; arch=$${t#*/}; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build -o /dev/null $(PKGS) || exit 1; \
		echo "cross $$t: 通过"; \
	done

# TST-012：每次运行短时 fuzz；夜间长时 fuzz 见 fuzz-long
.PHONY: fuzz-short
fuzz-short:
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzDecode -fuzztime=10s
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzReadFixedHeader -fuzztime=5s
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzValidateUTF8 -fuzztime=5s
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzTrieAgreesWithPairwise -fuzztime=10s
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzValidateFilter -fuzztime=5s
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzValidateName -fuzztime=5s
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzMatch -fuzztime=5s
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzClassify -fuzztime=5s
	@$(GO) test $(AUTHZ_PKG) -run=XXX -fuzz=FuzzParseDocument -fuzztime=10s
	@$(GO) test $(AUTHZ_PKG) -run=XXX -fuzz=FuzzGlobMatch -fuzztime=5s
	@$(GO) test $(AUTHZ_PKG) -run=XXX -fuzz=FuzzEvalNeverPanics -fuzztime=10s
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzMergeKeepsInvariants -fuzztime=10s
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzDecodeNeverPanics -fuzztime=5s
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzParseRequestNeverPanics -fuzztime=5s
	@echo "fuzz-short: 通过"

.PHONY: fuzz-long
fuzz-long:
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzDecode -fuzztime=$(FUZZTIME)
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzReadFixedHeader -fuzztime=$(FUZZTIME)
	@$(GO) test $(PACKET_PKG) -run=XXX -fuzz=FuzzValidateUTF8 -fuzztime=$(FUZZTIME)
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzTrieAgreesWithPairwise -fuzztime=$(FUZZTIME)
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzValidateFilter -fuzztime=$(FUZZTIME)
	@$(GO) test $(TOPIC_PKG) -run=XXX -fuzz=FuzzMatch -fuzztime=$(FUZZTIME)
	@$(GO) test $(AUTHZ_PKG) -run=XXX -fuzz=FuzzParseDocument -fuzztime=$(FUZZTIME)
	@$(GO) test $(AUTHZ_PKG) -run=XXX -fuzz=FuzzEvalNeverPanics -fuzztime=$(FUZZTIME)
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzMergeKeepsInvariants -fuzztime=$(FUZZTIME)
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzDecodeNeverPanics -fuzztime=$(FUZZTIME)
	@$(GO) test $(SHADOW_PKG) -run=XXX -fuzz=FuzzParseRequestNeverPanics -fuzztime=$(FUZZTIME)

# TST-003、TST-017、TST-030 至 TST-034：确定性模拟
#
# 门禁跑的是固定种子集（见 simulation_test.go 的 defaultSimSeeds），
# 已包含在 test / test-race 里。这个目标是夜间用的随机种子扫描：
# 失败时日志里会打印可直接重放的命令。
.PHONY: sim
sim:
	@$(GO) test ./internal/broker/ -run TestSimulation -count=1 \
		-timeout 30m -sim.random=$(SIM_SEEDS) -sim.ops=$(SIM_OPS)

SIM_SEEDS ?= 50
SIM_OPS   ?= 3000

# TST-060 至 TST-064：性能基线
.PHONY: bench
bench:
	@$(GO) test $(BENCH_PKGS) -run=XXX -bench=. -benchmem

# TST-060、TST-061、TST-163：端到端基准测试。
#
# 与上面的 bench 不是一回事：bench 量的是函数（编解码、匹配、策略求值），
# 这个起真进程、按真 MQTT 连接压，量的是「一台机器上端到端能跑多少」。
# 不进 gate：要起进程、要 openssl（集群的复制证书）、要 python3，一轮几分钟。
#
# 参数与结果位置见 tests/bench/README.md；入库的那份在 docs/baseline/throughput/。
.PHONY: bench-e2e
bench-e2e:
	@bash tests/bench/run.sh

# TST-072：依赖漏洞扫描
.PHONY: vuln
vuln:
	@$(GO) run golang.org/x/vuln/cmd/govulncheck@latest $(PKGS)

# clean 删掉可以再生的产物。
#
# 它刻意**不碰**三样东西：
#
#   - internal/server/consoleassets/dist 是故意入库的（UI-039：没有 Node
#     时 Go 侧构建不能被阻塞），删了就得装 Node 才能再起服务；
#   - node_modules 重装要几分钟还要联网，而它不占仓库；
#   - .demo/ 归 demo-clean，而且里面那个进程可能正跑着。
#
# 名单写全是有意义的：此前这里只清覆盖率文件，于是根目录一个 10 MB 的
# 旧二进制和一份前端中间产物一直躺着，而两者都在 .gitignore 里——也就是
# 说谁都不认为它们该留下，只是没人删。
# ---------- 容器 ----------

IMAGE ?= open-iot-core:dev

# 本地构建镜像。发布用的多架构镜像由 tag 触发的 release workflow 推到
# ghcr.io，这里只是让本地能试，不必等 CI。
.PHONY: image
image:
	@docker build -f deploy/Dockerfile -t $(IMAGE) .
	@echo "image: $(IMAGE)"

.PHONY: clean
clean:
	@rm -f $(COVERFILE) $(COVERFILE).strict coverage.html
	@rm -f openiotd openiotctl mqttbench
	@find . -name '*.test' -not -path '*/node_modules/*' -delete
	@rm -rf $(CONSOLE_DIR)/dist
	@find . -name .DS_Store -not -path '*/node_modules/*' -delete
	@rm -rf deploy/cluster/compose/pki deploy/cluster/k8s/pki
	@echo "clean: 已清理可再生产物（入库的控制台产物、node_modules 与 .demo 不动）"

# ---------- 控制台 ----------

CONSOLE_DIR  := web/console
CONSOLE_DIST := internal/server/consoleassets/dist

# 重建前端产物。
#
# 教程正文（docs/tutorial/{zh,en}/ 的 markdown，UI-078）由 vite 以 ?raw
# 打进同一份产物，所以**改了教程也要跑这个**——门禁会拿已提交的产物比对
# 页标题（TST-142），忘了就会红。
.PHONY: console
console:
	@command -v npm >/dev/null 2>&1 || { \
		echo "console: 需要 Node 与 npm。产物已随仓库提交，常规构建无需本步骤（PRD UI-039）"; \
		exit 1; }
	@cd $(CONSOLE_DIR) && npm ci --silent 2>/dev/null || (cd $(CONSOLE_DIR) && npm install --silent)
	@cd $(CONSOLE_DIR) && npm run build
	@rm -rf $(CONSOLE_DIST)
	@mkdir -p $(CONSOLE_DIST)
	@cp -r $(CONSOLE_DIR)/dist/. $(CONSOLE_DIST)/
	@echo "console: 产物已更新到 $(CONSOLE_DIST)"
	@printf "console: gzip 合计 "
	@cat $(CONSOLE_DIST)/assets/* $(CONSOLE_DIST)/index.html | gzip -c | wc -c | tr -d ' '

# UI-059、UI-060、TST-131：在真实浏览器里把控制台点一遍。
# 不进 gate：门禁只依赖 Go 工具链，而这套要 Node 与 Chromium
# （与 tests/device-sdk 同一条理由）。改过 web/console/src 之后跑它。
.PHONY: console-e2e
console-e2e:
	@bash tests/console-e2e/run.sh

# TST-144：教程里三份 SDK 示例对一个临时节点真的跑一遍。
# 不进 gate：要 JDK、Maven、Python 与网络（各语言的 SDK 要现拉），
# 而门禁只依赖 Go 工具链。缺哪个语言的工具链就跳过那一份并明说。
.PHONY: examples-test
examples-test:
	@bash tests/examples/run.sh

# CLU-064 至 CLU-074：三个真进程组成元数据集群的验收。
# 不进 gate：要 openssl 并且要起三个进程等选举。
.PHONY: cluster-test
cluster-test:
	@bash tests/cluster/run.sh

.PHONY: console-check
console-check:
	@command -v npm >/dev/null 2>&1 || { echo "console-check: 跳过（无 Node）"; exit 0; }
	@cd $(CONSOLE_DIR) && npm run typecheck
	@echo "console-check: 类型检查通过"

.PHONY: help
help:
	@echo "常用目标："
	@echo "  make sim           确定性模拟的随机种子扫描（夜间用，门禁跑固定种子）"
	@echo "  make demo          一键启动演示环境（建 CA、签证书、预置数据、起服务）"
	@echo "  make demo-clean    删除演示数据目录 .demo/"
	@echo "  make clean         清理可再生产物（覆盖率、二进制、前端中间产物）"
	@echo "  make image         本地构建容器镜像（部署清单见 deploy/）"
	@echo "  make console       重建前端（需要 Node；产物已提交，常规构建无需此步）"
	@echo "  make console-check 前端类型检查"
	@echo "  make console-e2e   在真实浏览器里点一遍控制台（需要 Node 与 Chromium）"
	@echo "  make examples-test 教程里的 SDK 示例真跑一遍（需要 Python/JDK 与网络）"
	@echo "  make cluster-test  三个真进程组成元数据集群的验收（需要 openssl 与 python3）"
	@echo "  make bench         微基准（编解码、主题匹配、策略求值）"
	@echo "  make bench-e2e     端到端基准：单节点与三节点集群各跑一组场景"
	@echo "  make gate          完整门禁（fmt/vet/契约/竞态/覆盖率/交叉编译/fuzz）"
	@echo ""
	@echo "演示环境可用环境变量覆盖端口与产物目录："
	@echo "  MQTT_PORT=18883 ADMIN_PORT=19090 make demo"
	@echo "  DEMO_DIR=.demo2 make demo        # 与已有的那份并存"

.PHONY: demo
demo:
	@./scripts/demo.sh

.PHONY: demo-clean
demo-clean:
	@# 空值守卫。DEMO_DIR 被设成空串时下面那条 rm -rf 会退化成一条
	@# 没有目标的命令，而再加一个斜杠就是 rm -rf /——宁可直接拒绝。
	@test -n "$(DEMO_DIR)" || { echo "demo-clean: DEMO_DIR 不能为空"; exit 1; }
	@# 先停进程再删目录。反过来会把数据目录从一个正在运行的节点脚下
	@# 删掉，而它继续占着端口——下一次 make demo 于是撞上"端口被占用"，
	@# 而用户刚跑过的恰恰是那条名字听起来就该清掉这些的命令。
	@if pgrep -f '$(DEMO_DIR)/openiotd' >/dev/null 2>&1; then \
		pkill -f '$(DEMO_DIR)/openiotd' && echo "demo-clean: 已停止运行中的 demo"; \
		sleep 1; \
	fi
	@rm -rf $(DEMO_DIR)
	@echo "demo-clean: 已删除 $(DEMO_DIR)/"
