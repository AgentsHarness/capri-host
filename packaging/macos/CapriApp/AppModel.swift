import AppKit
import Combine
import Darwin
import Foundation

final class AppModel: ObservableObject {
    static let shared = AppModel()

    @Published var statusText = "已停止"
    @Published var toggleTitle = "启动 Host"
    @Published var toggleDisabled = false
    @Published var restartDisabled = true
    @Published var startAtLogin = false
    @Published var keepAwake = false

    @Published var hubSnapshot: HubSnapshot?
    @Published var lanBound = false

    private let host = HostProcess()
    private let sleepInhibitor = SleepInhibitor()
    private var timer: Timer?
    private var hubFetchGen = 0
    private var hubFetchBusy = false
    private var lastStartAtLoginPref: Bool?
    private var lastKeepAwakePref: Bool?
    private var configWatch: DispatchSourceFileSystemObject?

    private init() {}

    func bootstrap() {
        appLog("Capri-app \(capriVersion) launched")

        var f = ConfigFile.load()
        // 配置里还没写过这个键、但系统登录项已经启用（例如用户在「系统设置」
        // 里手动开启）时，把它写回配置，保证网页设置页显示的不是过期值。
        if Autostart.isEnabled && f.startAtLogin == nil {
            try? ConfigFile.update { $0.startAtLogin = true }
            f.startAtLogin = true
        }
        lastStartAtLoginPref = f.shouldStartAtLogin
        lastKeepAwakePref = f.shouldKeepAwake
        if f.shouldStartAtLogin {
            try? Autostart.setEnabled(true)
        }
        if f.shouldKeepAwake {
            _ = sleepInhibitor.enable()
        }
        startAtLogin = Autostart.isEnabled
        keepAwake = sleepInhibitor.isEnabled

        startConfigWatcher()
        refreshStatus()
        timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { [weak self] _ in
            self?.refreshStatus()
        }
        if let t = timer {
            RunLoop.main.add(t, forMode: .common)
        }

        if !ConfigFile.exists() {
            openSettings()
            return
        }
        if f.shouldStartHostOnLaunch {
            startHost()
        }
    }

    /// 设置页保存后会原子替换 config.json。监听目录而不是轮询文件：
    /// 写临时文件再 rename 也能收到 write/rename 事件，平时没有额外开销。
    private func startConfigWatcher() {
        let dir = ConfigFile.configDir()
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let fd = open(dir.path, O_EVTONLY)
        guard fd >= 0 else {
            appLog("无法监听配置文件目录: \(dir.path)")
            return
        }
        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: fd,
            eventMask: [.write, .rename, .delete],
            queue: .main
        )
        source.setEventHandler { [weak self] in
            self?.syncSystemPrefs()
        }
        source.setCancelHandler {
            close(fd)
        }
        source.resume()
        configWatch = source
    }

    /// 把网页设置页写入的 start_at_login / keep_awake 落到系统登录项和
    /// 电源断言上。只在配置文件变化时应用，避免和用户在系统设置里做的
    /// 修改互相打架。
    private func syncSystemPrefs() {
        let f = ConfigFile.load()
        if lastStartAtLoginPref != f.shouldStartAtLogin {
            lastStartAtLoginPref = f.shouldStartAtLogin
            appLog("配置变更: start_at_login=\(f.shouldStartAtLogin)")
            do {
                try Autostart.setEnabled(f.shouldStartAtLogin)
            } catch {
                appLog("登录项设置失败: \(error.localizedDescription)")
            }
        }
        if lastKeepAwakePref != f.shouldKeepAwake {
            lastKeepAwakePref = f.shouldKeepAwake
            appLog("配置变更: keep_awake=\(f.shouldKeepAwake)")
            if f.shouldKeepAwake {
                if !sleepInhibitor.enable() {
                    appLog("开启休眠阻止失败")
                }
            } else {
                sleepInhibitor.disable()
            }
        }

        let login = Autostart.isEnabled
        if startAtLogin != login { startAtLogin = login }
        let awake = sleepInhibitor.isEnabled
        if keepAwake != awake { keepAwake = awake }
    }

    func refreshStatus() {
        let f = ConfigFile.load()
        let login = Autostart.isEnabled
        if startAtLogin != login { startAtLogin = login }
        let awake = sleepInhibitor.isEnabled
        if keepAwake != awake { keepAwake = awake }
        if lanBound != f.isLAN { lanBound = f.isLAN }
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
        let canRestart = ours
        if restartDisabled != !canRestart { restartDisabled = !canRestart }
    }

    func toggleHost() {
        if host.isRunning {
            stopHost()
        } else {
            startHost()
        }
    }

    func startHost() {
        statusText = "正在启动…"
        toggleTitle = "停止 Host"
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            do {
                try self.host.start()
                let ok = self.host.waitUntilListening(timeout: 12)
                DispatchQueue.main.async {
                    if !ok {
                        let err = self.host.lastError.isEmpty ? "等待端口监听超时" : self.host.lastError
                        appLog("启动 Host 失败: \(err)")
                    }
                    self.refreshStatus()
                }
            } catch {
                DispatchQueue.main.async {
                    appLog("启动 Host 失败: \(error.localizedDescription)")
                    self.refreshStatus()
                }
            }
        }
    }

    func stopHost() {
        guard host.isRunning else {
            refreshStatus()
            return
        }
        statusText = "正在停止…"
        toggleDisabled = true
        restartDisabled = true
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            self.host.stop()
            DispatchQueue.main.async {
                self.refreshStatus()
            }
        }
    }

    func stopHostOnTerminate() {
        host.stop()
    }

    func restartHost() {
        guard host.isRunning else {
            startHost()
            return
        }
        statusText = "正在重启…"
        toggleDisabled = true
        restartDisabled = true
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            self.host.stop()
            DispatchQueue.main.async {
                self.startHost()
            }
        }
    }

    func openWeb() {
        openWebPage(path: "/")
    }

    func openSettings() {
        // 设置页必须钉在这台 Host 上：hub 模式下没有 ?host= 时，hub 会
        // 转给「最近心跳的在线 Host」，多台在线时状态与保存都会飘到别的
        // 机器。把本机 hostId 显式带进 URL，页面启动时据此固定目标。
        var path = "/settings"
        if let id = ConfigFile.load().hostID?.trimmingCharacters(in: .whitespacesAndNewlines),
           !id.isEmpty,
           let encoded = id.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) {
            path += "?host=\(encoded)"
        }
        openWebPage(path: path)
    }

    private func openWebPage(path: String) {
        let filePort = ConfigFile.load().listenPort
        let portNum = (host.isRunning ? host.endpoint()?.port : nil) ?? filePort
        if HostProcess.isListening(port: portNum), let url = URL(string: "http://127.0.0.1:\(portNum)\(path)") {
            Paths.open(url)
            return
        }
        startHost()
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self else { return }
            _ = self.host.waitUntilListening(timeout: 12)
            DispatchQueue.main.async {
                let port = self.host.endpoint()?.port ?? filePort
                if HostProcess.isListening(port: port), let url = URL(string: "http://127.0.0.1:\(port)\(path)") {
                    Paths.open(url)
                } else {
                    self.showStartFailure()
                }
                self.refreshStatus()
            }
        }
    }

    private func showStartFailure() {
        appLog("无法打开设置页：Host 未就绪")
        let alert = NSAlert()
        alert.messageText = "Host 未能启动"
        alert.informativeText = "设置页由本机 Host 提供。请从托盘菜单打开日志，检查端口占用后重试。"
        alert.alertStyle = .warning
        alert.addButton(withTitle: "好")
        NSApp.activate(ignoringOtherApps: true)
        alert.runModal()
    }

    func copyLANAddress() {
        let filePort = ConfigFile.load().listenPort
        let port = (host.isRunning ? host.endpoint()?.port : nil) ?? filePort
        guard let ip = LANAddress.preferredIPv4() else {
            appLog("复制局域网地址失败：找不到可用的局域网地址")
            return
        }
        let text = "http://\(ip):\(port)/"
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
        appLog("已复制局域网地址 \(text)")
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
                // uptimeSec 每秒 +1，菜单不显示。整份写回 @Published
                // 会让 MenuBarExtra 每拍再登记一份观察且不释放。
                if let result {
                    if self.hubSnapshot?.ignoringUptime() != result.ignoringUptime() {
                        self.hubSnapshot = result
                    }
                } else if self.hubSnapshot != nil {
                    self.hubSnapshot = nil
                }
            }
        }
    }

    private func invalidateHubFetch() {
        hubFetchGen += 1
        if hubSnapshot != nil { hubSnapshot = nil }
    }

    func openLogs() {
        try? Paths.ensureLogDir()
        Paths.open(Paths.logDir())
    }

    func setStartAtLogin(_ on: Bool) {
        do {
            try Autostart.setEnabled(on)
            startAtLogin = Autostart.isEnabled
            try ConfigFile.update { $0.startAtLogin = on }
            lastStartAtLoginPref = on
        } catch {
            appLog("登录项设置失败: \(error.localizedDescription)")
            startAtLogin = Autostart.isEnabled
            lastStartAtLoginPref = ConfigFile.load().shouldStartAtLogin
        }
    }

    func setKeepAwake(_ on: Bool) {
        if on {
            if !sleepInhibitor.enable() {
                appLog("开启休眠阻止失败")
            }
        } else {
            sleepInhibitor.disable()
        }
        keepAwake = sleepInhibitor.isEnabled
        let target = keepAwake
        do {
            try ConfigFile.update { $0.keepAwake = target }
            lastKeepAwakePref = target
            appLog(target ? "已开启休眠阻止（屏幕可正常息屏）" : "已恢复系统休眠设置")
        } catch {
            appLog("保存休眠设置失败: \(error.localizedDescription)")
        }
    }
}

extension String {
    var nilIfEmpty: String? {
        let t = trimmingCharacters(in: .whitespacesAndNewlines)
        return t.isEmpty ? nil : t
    }
}
