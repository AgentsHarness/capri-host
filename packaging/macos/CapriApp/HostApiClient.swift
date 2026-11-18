import Foundation

struct HubSnapshot: Codable, Equatable {
    var configured: Bool
    var hubUrl: String?
    var hostId: String?
    var hostName: String?
    var paired: Bool
    var connected: Bool
    var transport: String?
    var connectedSince: String?
    var uptimeSec: Int64?
    var lastError: String?
    var storedUrl: String?
    var canReuse: Bool?

    /// 菜单和设置都不显示 uptimeSec，而它每次轮询都 +1。
    /// 比较前抹掉，避免相同界面状态被当成新快照。
    func ignoringUptime() -> HubSnapshot {
        var copy = self
        copy.uptimeSec = nil
        return copy
    }

    enum CodingKeys: String, CodingKey {
        case configured
        case hubUrl
        case hostId
        case hostName
        case paired
        case connected
        case transport
        case connectedSince
        case uptimeSec
        case lastError
        case storedUrl
        case canReuse
    }
}

private struct ApiResponse<T: Codable>: Codable {
    var ok: Bool
    var error: String?
    var version: String?
    var hub: T?
}

enum HostApiClient {
    static func fetchHubState(port: Int, token: String, timeout: TimeInterval = 1.5) async throws -> HubSnapshot {
        guard let url = URL(string: "http://127.0.0.1:\(port)/api/hub/state") else {
            throw URLError(.badURL)
        }
        var req = URLRequest(url: url, timeoutInterval: timeout)
        if !token.isEmpty {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let (data, res) = try await URLSession.shared.data(for: req)
        guard let http = res as? HTTPURLResponse, http.statusCode == 200 else {
            throw AppError("HTTP status != 200")
        }
        let parsed = try JSONDecoder().decode(ApiResponse<HubSnapshot>.self, from: data)
        guard let hub = parsed.hub else {
            throw AppError(parsed.error ?? "缺少 hub 状态")
        }
        return hub
    }

    static func quit(port: Int, token: String) async {
        guard let url = URL(string: "http://127.0.0.1:\(port)/api/host/quit") else { return }
        var req = URLRequest(url: url, timeoutInterval: 2.0)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if !token.isEmpty {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        req.httpBody = "{}".data(using: .utf8)
        _ = try? await URLSession.shared.data(for: req)
    }
}
