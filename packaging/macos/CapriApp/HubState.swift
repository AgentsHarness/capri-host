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

    /// 与 Go 侧 `st.URL == c.cfg.URL` 对齐：去空白后精确相等。
    func matches(hubURL: String) -> Bool {
        guard hasToken else { return false }
        let a = (url ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let b = hubURL.trimmingCharacters(in: .whitespacesAndNewlines)
        return !a.isEmpty && a == b
    }
}
