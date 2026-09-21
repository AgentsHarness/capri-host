import AppKit
import Combine
import Foundation

final class AppModel: ObservableObject {
    static let shared = AppModel()

    @Published var statusText = "已停止"
    @Published var toggleTitle = "启动 Host"
    @Published var toggleDisabled = false
    @Published var startAtLogin = false
    @Published var keepAwake = false
    @Published var hint = ""

    @Published var hostName = ""
    @Published var hostID = ""
    @Published var port = "8765"
    @Published var bindLAN = false
    @Published var feToken = ""
    @Published var hubURL = ""
    @Published var pairCode = ""
    @Published var grokBin = ""
    @Published var proxy = ""
    @Published var noProxy = ""
    @Published var startHostOnLaunch = true
    @Published var hubTokenReady = false
    @Published var hubTokenCaption = ""
    @Published var rePairExpanded = false

    @Published var hubSnapshot: HubSnapshot?
    @Published var lanBound = false

    private let host = HostProcess()
    private let sleepInhibitor = SleepInhibitor()
    private var timer: Timer?
    private var hubFetchGen = 0
    private var hubFetchBusy = false

    private init() {}

    func bootstrap() {
        appLog("Capri-app \(capriVersion) launched")
        loadForm()
        startAtLogin = Autostart.isEnabled || ConfigFile.load().shouldStartAtLogin
        keepAwake = sleepInhibitor.isEnabled || ConfigFile.load().shouldKeepAwake
        if keepAwake {
            _ = sleepInhibitor.enable()
        }
        refreshStatus()
        timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { [weak self] _ in
            self?.refreshStatus()
        }
        if let t = timer {
            RunLoop.main.add(t, forMode: .common)
        }

        if !ConfigFile.exists() {
            SettingsWindow.show()
            return
        }
        if startAtLogin {
            try? Autostart.setEnabled(true)
        }
        if ConfigFile.load().shouldStartHostOnLaunch {
            startHost()
        }
    }

    func loadForm() {
        let f = ConfigFile.load()
        hostName = f.hostName?.nilIfEmpty ?? Paths.defaultHostName()
        hostID = f.hostID?.nilIfEmpty ?? Paths.defaultHostID()
        port = String(f.listenPort)
        bindLAN = f.isLAN
        feToken = f.feToken ?? ""
        hubURL = f.hubURL?.nilIfEmpty ?? HubState.load()?.url ?? ""
        grokBin = f.grokBin ?? ""
        proxy = f.proxy ?? ""
        noProxy = f.noProxy ?? ""
        startHostOnLaunch = f.shouldStartHostOnLaunch
        startAtLogin = f.shouldStartAtLogin || Autostart.isEnabled
        keepAwake = sleepInhibitor.isEnabled || f.shouldKeepAwake
        hint = ""
        rePairExpanded = false
        refreshHubTokenState()
        if hubTokenReady {
            pairCode = ""
        } else {
            pairCode = f.hubPairCode ?? ""
        }
    }

    func refreshHubTokenState() {
        let typed = HubURL.normalize(hubURL)
        let st = HubState.load()
        let storedRaw = st?.url?.nilIfEmpty ?? hubSnapshot?.storedUrl?.nilIfEmpty ?? ""
        let stored = HubURL.normalize(storedRaw)

        if typed.isEmpty {
            if st?.hasToken == true {
                hubTokenReady = true
                hubTokenCaption = "已有可用 token（\(stored.isEmpty ? "hub.json" : stored)），启动 Host 会自动连上。"
            } else {
                hubTokenReady = false
                hubTokenCaption = "一次性配对码。成功后写在 ~/.capri-host/hub.json，之后不用再填。"
            }
            return
        }
        if st?.matches(hubURL: typed) == true {
            hubTokenReady = true
            if host.isRunning, ConfigFile.load().hubURL?.nilIfEmpty != nil {
                hubTokenCaption = "已有可用 token，Host 会用它连 Hub，不必再填配对码。"
            } else {
                hubTokenCaption = "已有可用凭证（\(stored)），可直接连上或更换配对。"
            }
            return
        }
        hubTokenReady = false
        if !stored.isEmpty {
            hubTokenCaption = "hub.json 里的 token 绑定的是 \(stored)，和当前 Hub URL 不一致，需要新配对码。"
        } else {
            hubTokenCaption = "一次性配对码。成功后写在 ~/.capri-host/hub.json，之后不用再填。"
        }
    }

    func refreshStatus() {
        let f = ConfigFile.load()
        lanBound = f.isLAN
        let ours = host.isRunning
        let portNum = (ours ? host.endpoint()?.port : nil) ?? f.listenPort
        let listen = HostProcess.isListening(port: portNum)
        let text: String
        let title: String
        let disabled: Bool
        if ours && listen {
            title = "停止 Host"
            disabled = false
            if let snap = hubSnapshot {
                if !snap.configured {
                    text = "运行中 :\(portNum)"
                } else if !snap.paired {
                    text = "运行中 :\(portNum) · 未配对"
                } else if snap.connected {
                    if let tr = snap.transport?.nilIfEmpty {
                        text = "运行中 :\(portNum) · Hub (\(tr.uppercased()))"
                    } else {
                        text = "运行中 :\(portNum) · Hub"
                    }
                } else if snap.lastError?.nilIfEmpty != nil {
                    text = "运行中 :\(portNum) · Hub 未连接"
                } else {
                    text = "运行中 :\(portNum) · Hub 连接中…"
                }
            } else {
                text = f.hubURL?.nilIfEmpty == nil ? "运行中 :\(portNum)" : "运行中 :\(portNum) · Hub"
            }
            pollHub(port: portNum, token: (ours ? host.endpoint()?.token : nil) ?? f.feToken ?? "")
        } else if ours && !listen {
            text = "正在启动…"
            title = "停止 Host"
            disabled = false
            invalidateHubFetch()
        } else if !ours && listen {
            text = "端口 :\(portNum) 已被占用"
            title = "启动 Host"
            disabled = true
            invalidateHubFetch()
        } else if !host.lastError.isEmpty {
            text = "启动失败"
            title = "启动 Host"
            disabled = false
            invalidateHubFetch()
        } else {
            text = "已停止"
            title = "启动 Host"
            disabled = false
            invalidateHubFetch()
        }
        if statusText != text { statusText = text }
        if toggleTitle != title { toggleTitle = title }
        if toggleDisabled != disabled { toggleDisabled = disabled }
    }

    func toggleHost() {
        if host.isRunning {
            stopHost()
            refreshStatus()
            return
        }
        startHost()
    }

    func startHost() {
        hint = ""
        statusText = "正在启动…"
        toggleTitle = "停止 Host"
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            do {
                try self.host.start()
                let ok = self.host.waitUntilListening(timeout: 12)
                DispatchQueue.main.async {
                    if ok {
                        let port = self.host.endpoint()?.port ?? ConfigFile.load().listenPort
                        self.hint = "Host 已在 :\(port) 运行"
                    } else {
                        let err = self.host.lastError.isEmpty ? "等待端口监听超时" : self.host.lastError
                        self.hint = err
                    }
                    self.refreshStatus()
                }
            } catch {
                DispatchQueue.main.async {
                    self.hint = error.localizedDescription
                    self.refreshStatus()
                }
            }
        }
    }

    func stopHost() {
        host.stop()
        refreshStatus()
    }

    func openWeb() {
        let filePort = ConfigFile.load().listenPort
        let portNum = (host.isRunning ? host.endpoint()?.port : nil) ?? filePort
        if HostProcess.isListening(port: portNum), let url = URL(string: "http://127.0.0.1:\(portNum)/") {
            Paths.open(url)
            return
        }
        startHost()
        DispatchQueue.global(qos: .userInitiated).async {
            _ = self.host.waitUntilListening(timeout: 12)
            DispatchQueue.main.async {
                let port = self.host.endpoint()?.port ?? filePort
                if HostProcess.isListening(port: port), let url = URL(string: "http://127.0.0.1:\(port)/") {
                    Paths.open(url)
                } else {
                    SettingsWindow.show()
                }
                self.refreshStatus()
            }
        }
    }

    func copyLANAddress() {
        let filePort = ConfigFile.load().listenPort
        let port = (host.isRunning ? host.endpoint()?.port : nil) ?? filePort
        guard let ip = LANAddress.preferredIPv4() else {
            hint = "找不到可用的局域网地址"
            return
        }
        let text = "http://\(ip):\(port)/"
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
        hint = "已复制 \(text)"
    }

    private func liveAPI() -> (port: Int, token: String) {
        if host.isRunning, let ep = host.endpoint() {
            return ep
        }
        let f = ConfigFile.load()
        return (f.listenPort, f.feToken ?? "")
    }

    private func pollHub(port: Int, token: String) {
        if hubFetchBusy { return }
        hubFetchBusy = true
        hubFetchGen += 1
        let gen = hubFetchGen
        Task {
            let result = try? await HostApiClient.fetchHubState(port: port, token: token)
            await MainActor.run {
                self.hubFetchBusy = false
                guard gen == self.hubFetchGen else { return }
                self.hubSnapshot = result
                self.refreshHubTokenState()
            }
        }
    }

    private func invalidateHubFetch() {
        hubFetchGen += 1
        hubSnapshot = nil
    }

    func openLogs() {
        try? Paths.ensureLogDir()
        Paths.open(Paths.logDir())
    }

    func detectGrok() {
        if let found = Paths.discoverGrok() {
            grokBin = found
            hint = "找到 \(found)"
        } else {
            hint = "没有找到 grok，请先安装 Grok Build 或填绝对路径"
        }
    }

    func setStartAtLogin(_ on: Bool) {
        do {
            try Autostart.setEnabled(on)
            startAtLogin = on
            try ConfigFile.update { $0.startAtLogin = on }
        } catch {
            hint = "登录项: \(error.localizedDescription)"
            startAtLogin = Autostart.isEnabled
            SettingsWindow.show()
        }
    }

    func setKeepAwake(_ on: Bool) {
        if on {
            _ = sleepInhibitor.enable()
        } else {
            sleepInhibitor.disable()
        }
        keepAwake = sleepInhibitor.isEnabled
        try? ConfigFile.update { $0.keepAwake = on }
        hint = keepAwake ? "已开启休眠阻止（屏幕可正常息屏）" : "已恢复系统休眠设置"
    }

    func onlinePair() {
        let ep = liveAPI()
        let portNum = ep.port
        let token = ep.token
        let code = pairCode.trimmingCharacters(in: .whitespacesAndNewlines)
        let url = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !code.isEmpty else {
            hint = "请填写 6 位配对码"
            return
        }
        hint = "正在配对 Hub…"
        Task {
            do {
                let hub = try await HostApiClient.pair(port: portNum, token: token, hubURL: url, code: code)
                await MainActor.run {
                    self.hubSnapshot = hub
                    self.pairCode = ""
                    self.rePairExpanded = false
                    self.hint = "配对成功，已连上 Hub：\(hub.hubUrl ?? url)"
                    self.refreshStatus()
                }
            } catch {
                await MainActor.run {
                    self.hint = "配对失败: \(error.localizedDescription)"
                }
            }
        }
    }

    func onlineReuse() {
        let ep = liveAPI()
        let portNum = ep.port
        let token = ep.token
        let url = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
        hint = "正在使用已有凭证连接 Hub…"
        Task {
            do {
                let hub = try await HostApiClient.reuse(port: portNum, token: token, hubURL: url)
                await MainActor.run {
                    self.hubSnapshot = hub
                    self.hint = "已用存留凭证连上 Hub：\(hub.hubUrl ?? url)"
                    self.refreshStatus()
                }
            } catch {
                await MainActor.run {
                    self.hint = "复用凭证失败: \(error.localizedDescription)"
                }
            }
        }
    }

    func onlineDisconnect() {
        let ep = liveAPI()
        let portNum = ep.port
        let token = ep.token
        hint = "正在断开 Hub…"
        Task {
            do {
                let hub = try await HostApiClient.disconnect(port: portNum, token: token)
                await MainActor.run {
                    self.hubSnapshot = hub
                    self.hubURL = ""
                    self.pairCode = ""
                    self.hint = "已断开 Hub，切回仅本机模式"
                    self.refreshStatus()
                }
            } catch {
                await MainActor.run {
                    self.hint = "断开失败: \(error.localizedDescription)"
                }
            }
        }
    }

    func save(andStart: Bool) {
        guard let portNum = Int(port.trimmingCharacters(in: .whitespaces)), portNum > 0 else {
            hint = "端口必须是正整数"
            return
        }
        let trimmedName = hostName.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? Paths.defaultHostName()
        let runningName = hubSnapshot?.hostName?.nilIfEmpty
            ?? ConfigFile.load().hostName?.nilIfEmpty
            ?? Paths.defaultHostName()
        var draft = ConfigFile.load()
        draft.bind = bindLAN ? "0.0.0.0" : "127.0.0.1"
        draft.port = portNum
        draft.feToken = feToken
        if let err = draft.bindPolicyError() {
            hint = err
            return
        }
        let newPair = pairCode.trimmingCharacters(in: .whitespacesAndNewlines)
        let replacePair = !newPair.isEmpty && (rePairExpanded || !hubTokenReady)
        if replacePair {
            HubState.clear()
        }
        do {
            try ConfigFile.update { f in
                f.bind = bindLAN ? "0.0.0.0" : "127.0.0.1"
                f.port = portNum
                f.hostName = trimmedName
                f.hubURL = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
                f.hubPairCode = replacePair ? newPair : nil
                f.feToken = feToken
                f.grokBin = grokBin.trimmingCharacters(in: .whitespacesAndNewlines)
                f.proxy = proxy.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
                f.noProxy = noProxy.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
                f.startHostOnLaunch = startHostOnLaunch
                f.startAtLogin = startAtLogin
                f.keepAwake = keepAwake
            }
            try Autostart.setEnabled(startAtLogin)
            hint = "已保存 \(ConfigFile.path().path)"
            hostName = trimmedName
            refreshHubTokenState()
            if hubTokenReady {
                rePairExpanded = false
                pairCode = ""
            }
        } catch {
            hint = "保存失败: \(error.localizedDescription)"
            return
        }

        if host.isRunning && trimmedName != runningName {
            let ep = liveAPI()
            Task {
                _ = try? await HostApiClient.rename(port: ep.port, token: ep.token, name: trimmedName)
            }
        }

        refreshStatus()
        if andStart {
            if host.isRunning { stopHost() }
            startHost()
        }
    }
}

extension String {
    var nilIfEmpty: String? {
        let t = trimmingCharacters(in: .whitespacesAndNewlines)
        return t.isEmpty ? nil : t
    }
}
