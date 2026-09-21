import AppKit
import Foundation

enum Paths {
    static func logDir() -> URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Logs/Capri")
    }

    static func hostLog() -> URL { logDir().appendingPathComponent("Capri-host.log") }
    static func appLog() -> URL { logDir().appendingPathComponent("Capri-app.log") }

    static func defaultHostName() -> String {
        Host.current().localizedName ?? Host.current().name ?? "Local Host"
    }

    static func defaultHostID() -> String {
        sanitizeHostID(defaultHostName())
    }

    static func sanitizeHostID(_ raw: String) -> String {
        let lower = raw.lowercased()
        var out = ""
        var dash = false
        for ch in lower {
            if ch.isLetter || ch.isNumber {
                out.append(ch)
                dash = false
            } else if !out.isEmpty && !dash {
                out.append("-")
                dash = true
            }
        }
        let trimmed = out.trimmingCharacters(in: CharacterSet(charactersIn: "-"))
        return trimmed.isEmpty ? "local" : trimmed
    }

    static func findHostBinary() throws -> URL {
        if let env = ProcessInfo.processInfo.environment["CAPRI_HOST_BIN"], !env.isEmpty {
            let url = URL(fileURLWithPath: env)
            if FileManager.default.isExecutableFile(atPath: url.path) { return url }
            throw AppError("CAPRI_HOST_BIN=\(env) 不存在")
        }
        var candidates: [URL] = []
        let bundleMacOS = Bundle.main.bundleURL
            .appendingPathComponent("Contents/MacOS/Capri-host")
        candidates.append(bundleMacOS)
        if let exe = Bundle.main.executableURL {
            candidates.append(exe.deletingLastPathComponent().appendingPathComponent("Capri-host"))
        }
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        candidates.append(cwd.appendingPathComponent("Capri-host"))
        candidates.append(cwd.appendingPathComponent("bin/Capri-host"))
        for url in candidates {
            if FileManager.default.isExecutableFile(atPath: url.path) { return url }
        }
        throw AppError("找不到 Capri-host（应在 Capri.app/Contents/MacOS/ 里）")
    }

    static func discoverGrok() -> String? {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let names = [
            home.appendingPathComponent(".local/bin/grok"),
            home.appendingPathComponent(".grok/bin/grok"),
            URL(fileURLWithPath: "/opt/homebrew/bin/grok"),
            URL(fileURLWithPath: "/usr/local/bin/grok"),
        ]
        for url in names {
            if FileManager.default.isExecutableFile(atPath: url.path) {
                return url.path
            }
        }
        return nil
    }

    static func childEnvironment() -> [String: String] {
        var env = ProcessInfo.processInfo.environment
        let home = FileManager.default.homeDirectoryForCurrentUser
        let extras = [
            home.appendingPathComponent(".local/bin").path,
            home.appendingPathComponent(".grok/bin").path,
            "/opt/homebrew/bin",
            "/usr/local/bin",
        ].filter { FileManager.default.fileExists(atPath: $0) }
        let current = env["PATH"] ?? "/usr/bin:/bin"
        let prefix = extras.filter { !current.split(separator: ":").map(String.init).contains($0) }
        if !prefix.isEmpty {
            env["PATH"] = (prefix + [current]).joined(separator: ":")
        }
        return env
    }

    static func open(_ url: URL) {
        NSWorkspace.shared.open(url)
    }

    static func ensureLogDir() throws {
        try FileManager.default.createDirectory(at: logDir(), withIntermediateDirectories: true)
    }
}

struct AppError: LocalizedError {
    var message: String
    init(_ message: String) { self.message = message }
    var errorDescription: String? { message }
}

func appLog(_ message: String) {
    let line = "\(ISO8601DateFormatter().string(from: Date())) \(message)\n"
    do {
        try Paths.ensureLogDir()
        let url = Paths.appLog()
        if !FileManager.default.fileExists(atPath: url.path) {
            FileManager.default.createFile(atPath: url.path, contents: nil)
        }
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        try handle.seekToEnd()
        if let data = line.data(using: .utf8) {
            try handle.write(contentsOf: data)
        }
    } catch {
        fputs(line, stderr)
    }
}
