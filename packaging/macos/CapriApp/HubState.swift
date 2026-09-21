import Foundation

/// host 配对成功后写的 ~/.capri-host/hub.json。token 绑定当时的 Hub URL，
/// 和 capri-host ensureToken 同一份文件：URL 对得上且 token 非空就会跳过配对码。
struct HubState: Codable {
    var url: String?
    var hostId: String?
    var token: String?

    enum CodingKeys: String, CodingKey {
        case url
        case hostId
        case token
    }

    static func path() -> URL {
        ConfigFile.configDir().appendingPathComponent("hub.json")
    }

    static func load() -> HubState? {
        guard let data = try? Data(contentsOf: path()) else { return nil }
        return try? JSONDecoder().decode(HubState.self, from: data)
    }

    static func clear() {
        try? FileManager.default.removeItem(at: path())
    }

    var hasToken: Bool {
        !(token ?? "").trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    /// Same address after the host's normalization (bare host means https).
    func matches(hubURL: String) -> Bool {
        guard hasToken else { return false }
        let a = HubURL.normalize(url ?? "")
        let b = HubURL.normalize(hubURL)
        return !a.isEmpty && a == b
    }
}

enum HubURL {
    /// Mirrors hubstate.NormalizeURL: trim, default scheme https, keep only the origin.
    static func normalize(_ raw: String) -> String {
        var s = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if s.isEmpty { return "" }
        if !s.contains("://") { s = "https://" + s }
        guard var c = URLComponents(string: s),
              c.scheme == "http" || c.scheme == "https",
              let host = c.host, !host.isEmpty else { return "" }
        c.path = ""
        c.query = nil
        c.fragment = nil
        c.user = nil
        c.password = nil
        return c.string ?? ""
    }
}
