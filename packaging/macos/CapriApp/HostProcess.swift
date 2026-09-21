import Foundation

final class HostProcess {
    private let lock = NSLock()
    private var process: Process?
    private var logHandle: FileHandle?
    private(set) var lastError: String = ""
    private var boundPort: Int = 0
    private var boundToken: String = ""

    var isRunning: Bool {
        lock.lock()
        defer { lock.unlock() }
        return process?.isRunning == true
    }

    /// Port and token this child was started with. Nil before the first start.
    func endpoint() -> (port: Int, token: String)? {
        lock.lock()
        defer { lock.unlock() }
        guard boundPort > 0 else { return nil }
        return (boundPort, boundToken)
    }

    func start() throws {
        let cfg = ConfigFile.load()
        let port = cfg.listenPort
        let token = cfg.feToken ?? ""
        lock.lock()
        defer { lock.unlock() }
        if process?.isRunning == true { return }
        if Self.isListening(port: port) {
            lastError = "端口 \(port) 已被占用（可能是外部 capri-host）"
            throw AppError(lastError)
        }
        let bin = try Paths.findHostBinary()
        try Paths.ensureLogDir()
        let logURL = Paths.hostLog()
        if !FileManager.default.fileExists(atPath: logURL.path) {
            FileManager.default.createFile(atPath: logURL.path, contents: nil)
        }
        let handle = try FileHandle(forWritingTo: logURL)
        try handle.seekToEnd()
        let stamp = "\n== capri-host start \(ISO8601DateFormatter().string(from: Date())) bin=\(bin.path) ==\n"
        if let data = stamp.data(using: .utf8) {
            try handle.write(contentsOf: data)
        }

        let proc = Process()
        proc.executableURL = bin
        proc.currentDirectoryURL = Paths.logDir()
        proc.environment = Paths.childEnvironment()
        proc.standardOutput = handle
        proc.standardError = handle
        proc.terminationHandler = { [weak self] finished in
            guard let self else { return }
            self.lock.lock()
            if self.process === finished {
                self.process = nil
                try? self.logHandle?.close()
                self.logHandle = nil
                if finished.terminationStatus != 0 {
                    self.lastError = "capri-host 退出码 \(finished.terminationStatus)"
                }
            }
            self.lock.unlock()
            appLog("capri-host exited status=\(finished.terminationStatus)")
        }
        try proc.run()
        process = proc
        logHandle = handle
        boundPort = port
        boundToken = token
        lastError = ""
        appLog("started capri-host pid=\(proc.processIdentifier) bin=\(bin.path)")
    }

    func stop() {
        lock.lock()
        let proc = process
        let port = boundPort
        let token = boundToken
        lock.unlock()
        guard let proc, proc.isRunning else { return }
        appLog("stopping capri-host pid=\(proc.processIdentifier)")

        // Quit the listener this child bound, not the port now written in the file.
        if port > 0 {
            Task {
                await HostApiClient.quit(port: port, token: token)
            }
        }

        let deadline = Date().addingTimeInterval(6)
        while Date() < deadline {
            if !proc.isRunning { return }
            Thread.sleep(forTimeInterval: 0.05)
        }
        if proc.isRunning {
            proc.interrupt()
            proc.terminate()
        }
    }

    func waitUntilListening(timeout: TimeInterval) -> Bool {
        let port = endpoint()?.port ?? ConfigFile.load().listenPort
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if Self.isListening(port: port) { return true }
            if !isRunning { return false }
            Thread.sleep(forTimeInterval: 0.15)
        }
        return Self.isListening(port: port)
    }

    static func isListening(port: Int) -> Bool {
        guard let url = URL(string: "http://127.0.0.1:\(port)/api/probe") else { return false }
        var req = URLRequest(url: url, timeoutInterval: 0.25)
        req.httpMethod = "GET"
        let sem = DispatchSemaphore(value: 0)
        var ok = false
        URLSession.shared.dataTask(with: req) { _, response, _ in
            ok = response != nil
            sem.signal()
        }.resume()
        _ = sem.wait(timeout: .now() + 0.4)
        return ok
    }
}
