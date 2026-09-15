import Foundation
import ServiceManagement

enum Autostart {
    static var isEnabled: Bool {
        SMAppService.mainApp.status == .enabled
    }

    static func setEnabled(_ on: Bool) throws {
        let service = SMAppService.mainApp
        if on {
            if service.status != .enabled {
                try service.register()
            }
        } else if service.status == .enabled {
            try service.unregister()
        }
    }
}
