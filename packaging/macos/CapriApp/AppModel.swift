import AppKit
import Combine
import Foundation

final class AppModel: ObservableObject {
    static let shared = AppModel()

    @Published var statusText = "已停止"
    @Published var toggleTitle = "启动 Host"
    @Published var toggleDisabled = false
    @Published var startAtLogin = false
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

    private let host = HostProcess()
    private var timer: Timer?

    private init() {}

    func bootstrap() {
        appLog("capri-app \(capriVersion) launched")
        loadForm()
        startAtLogin = Autostart.isEnabled || ConfigFile.load().shouldStartAtLogin
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
        let st = HubState.load()
        let url = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let st, st.hasToken else {
            hubTokenReady = false
            hubTokenCaption = "一次性配对码。成功后写在 ~/.capri-host/hub.json，之后不用再填。"
            return
        }
        let bound = (st.url ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if url.isEmpty {
            hubTokenReady = true
            hubTokenCaption = "已有可用 token（\(bound.isEmpty ? "hub.json" : bound)），启动 Host 会自动连上。"
            return
        }
        if st.matches(hubURL: url) {
            hubTokenReady = true
            if host.isRunning, ConfigFile.load().hubURL?.nilIfEmpty != nil {
                hubTokenCaption = "已有可用 token，Host 会用它连 Hub，不必再填配对码。"
            } else {
                hubTokenCaption = "已有可用 token，启动 Host 会自动连上，不必再填配对码。"
            }
            return
        }
        hubTokenReady = false
        hubTokenCaption = "hub.json 里的 token 绑定的是 \(bound.isEmpty ? "另一地址" : bound)，和当前 Hub URL 不一致，需要新配对码。"
    }

    func refreshStatus() {
        let f = ConfigFile.load()
        let port = f.listenPort
        let listen = HostProcess.isListening(port: port)
        let ours = host.isRunning
        if ours && listen {
            statusText = f.hubURL?.nilIfEmpty == nil ? "运行中 :\(port)" : "运行中 :\(port) · Hub"
            toggleTitle = "停止 Host"
            toggleDisabled = false
        } else if ours && !listen {
            statusText = "正在启动…"
            toggleTitle = "停止 Host"
            toggleDisabled = false
        } else if !ours && listen {
            statusText = "端口 :\(port) 已被占用"
            toggleTitle = "启动 Host"
            toggleDisabled = true
        } else if !host.lastError.isEmpty {
            statusText = "启动失败"
            toggleTitle = "启动 Host"
            toggleDisabled = false
        } else {
            statusText = "已停止"
            toggleTitle = "启动 Host"
            toggleDisabled = false
        }
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
                        self.hint = "Host 已在 :\(ConfigFile.load().listenPort) 运行"
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
        let port = ConfigFile.load().listenPort
        let url = URL(string: "http://127.0.0.1:\(port)/")!
        if HostProcess.isListening(port: port) {
            Paths.open(url)
            return
        }
        startHost()
        DispatchQueue.global(qos: .userInitiated).async {
            _ = self.host.waitUntilListening(timeout: 12)
            DispatchQueue.main.async {
                if HostProcess.isListening(port: port) {
                    Paths.open(url)
                } else {
                    SettingsWindow.show()
                }
                self.refreshStatus()
            }
        }
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
            var f = ConfigFile.load()
            f.startAtLogin = on
            try f.save()
        } catch {
            hint = "登录项: \(error.localizedDescription)"
            startAtLogin = Autostart.isEnabled
            SettingsWindow.show()
        }
    }

    func save(andStart: Bool) {
        guard let portNum = Int(port.trimmingCharacters(in: .whitespaces)), portNum > 0 else {
            hint = "端口必须是正整数"
            return
        }
        var f = ConfigFile()
        f.bind = bindLAN ? "0.0.0.0" : "127.0.0.1"
        f.port = portNum
        f.hostName = hostName.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? Paths.defaultHostName()
        f.hostID = hostID.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? Paths.defaultHostID()
        f.hubURL = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
        let newPair = pairCode.trimmingCharacters(in: .whitespacesAndNewlines)
        // 已有匹配 token 时 host 会忽略配对码；用户明确填了新码才清掉
        // hub.json，让下次启动走重新配对。
        if !newPair.isEmpty && (rePairExpanded || !hubTokenReady) {
            HubState.clear()
            f.hubPairCode = newPair
        } else {
            f.hubPairCode = nil
        }
        f.feToken = feToken
        f.grokBin = grokBin.trimmingCharacters(in: .whitespacesAndNewlines)
        f.proxy = proxy.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        f.noProxy = noProxy.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        f.startHostOnLaunch = startHostOnLaunch
        f.startAtLogin = startAtLogin
        if let err = f.bindPolicyError() {
            hint = err
            return
        }
        do {
            try f.save()
            try Autostart.setEnabled(startAtLogin)
            hint = "已保存 \(ConfigFile.path().path)"
            hostName = f.hostName ?? hostName
            hostID = f.hostID ?? hostID
            refreshHubTokenState()
            if hubTokenReady {
                rePairExpanded = false
                pairCode = ""
            }
        } catch {
            hint = "保存失败: \(error.localizedDescription)"
            return
        }
        refreshStatus()
        if andStart {
            if host.isRunning { stopHost() }
            startHost()
        }
    }
}

private extension String {
    var nilIfEmpty: String? {
        let t = trimmingCharacters(in: .whitespacesAndNewlines)
        return t.isEmpty ? nil : t
    }
}
