import Foundation

struct ConfigFile: Codable, Equatable {
    var bind: String?
    var port: Int?
    var hostID: String?
    var hostName: String?
    var hubURL: String?
    var hubPairCode: String?
    var feToken: String?
    var grokBin: String?
    var hubQUICPin: String?
    var startHostOnLaunch: Bool?
    var startAtLogin: Bool?

    enum CodingKeys: String, CodingKey {
        case bind, port
        case hostID = "host_id"
        case hostName = "host_name"
        case hubURL = "hub_url"
        case hubPairCode = "hub_pair_code"
        case feToken = "fe_token"
        case grokBin = "grok_bin"
        case hubQUICPin = "hub_quic_pin"
        case startHostOnLaunch = "start_host_on_launch"
        case startAtLogin = "start_at_login"
    }

    var listenPort: Int {
        if let port, port > 0 { return port }
        return 8765
    }

    var shouldStartHostOnLaunch: Bool { startHostOnLaunch ?? true }
    var shouldStartAtLogin: Bool { startAtLogin ?? false }

    var bindAddress: String {
        let v = (bind ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        return v.isEmpty ? "127.0.0.1" : v
    }

    var isLAN: Bool {
        let b = bindAddress
        return b != "127.0.0.1" && b != "localhost" && b != "::1"
    }

    func bindPolicyError() -> String? {
        let token = (feToken ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if isLAN && token.isEmpty {
            return "局域网访问必须填写本机钥匙（FE_TOKEN），或改回仅本机。"
        }
        return nil
    }

    static func configDir() -> URL {
        if let override = ProcessInfo.processInfo.environment["CAPRI_HOME"],
           !override.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return URL(fileURLWithPath: override)
        }
        return FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".capri-host")
    }

    static func path() -> URL {
        configDir().appendingPathComponent("config.json")
    }

    static func exists() -> Bool {
        FileManager.default.fileExists(atPath: path().path)
    }

    static func load() -> ConfigFile {
        let url = path()
        guard let data = try? Data(contentsOf: url) else { return ConfigFile() }
        return (try? JSONDecoder().decode(ConfigFile.self, from: data)) ?? ConfigFile()
    }

    func save() throws {
        let dir = ConfigFile.configDir()
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let enc = JSONEncoder()
        enc.outputFormatting = [.prettyPrinted, .sortedKeys]
        var data = try enc.encode(self)
        data.append(contentsOf: [0x0A])
        try data.write(to: ConfigFile.path(), options: .atomic)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600],
            ofItemAtPath: ConfigFile.path().path
        )
    }
}
