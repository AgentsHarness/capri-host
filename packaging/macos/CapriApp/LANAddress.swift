import Darwin
import Foundation

enum LANAddress {
    /// The IPv4 the default route would use. UDP connect sends no packet.
    static func preferredIPv4() -> String? {
        let fd = socket(AF_INET, SOCK_DGRAM, 0)
        if fd < 0 { return nil }
        defer { close(fd) }

        var remote = sockaddr_in()
        remote.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        remote.sin_family = sa_family_t(AF_INET)
        remote.sin_port = in_port_t(80).bigEndian
        guard inet_pton(AF_INET, "8.8.8.8", &remote.sin_addr) == 1 else { return nil }

        let connected = withUnsafePointer(to: &remote) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        if connected != 0 { return nil }

        var local = sockaddr_in()
        var len = socklen_t(MemoryLayout<sockaddr_in>.size)
        let named = withUnsafeMutablePointer(to: &local) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                getsockname(fd, $0, &len)
            }
        }
        if named != 0 { return nil }

        var buf = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
        var ip = local.sin_addr
        guard inet_ntop(AF_INET, &ip, &buf, socklen_t(INET_ADDRSTRLEN)) != nil else { return nil }
        let text = String(cString: buf)
        if text.isEmpty || text == "0.0.0.0" { return nil }
        return text
    }
}
