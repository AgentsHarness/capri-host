import Foundation
import IOKit.pwr_mgt

/// SleepInhibitor keeps the machine awake while enabled using IOKit power assertions.
/// The display can sleep normally, but the system will not suspend or idle sleep.
final class SleepInhibitor {
    private var assertionID: IOPMAssertionID = 0
    private(set) var isEnabled = false

    func enable(reason: String = "Capri-host 正在保持本机唤醒") -> Bool {
        guard !isEnabled else { return true }
        let type = kIOPMAssertionTypePreventUserIdleSystemSleep as CFString
        let result = IOPMAssertionCreateWithName(
            type,
            IOPMAssertionLevel(kIOPMAssertionLevelOn),
            reason as CFString,
            &assertionID
        )
        if result == kIOReturnSuccess {
            isEnabled = true
            return true
        }
        return false
    }

    func disable() {
        guard isEnabled else { return }
        IOPMAssertionRelease(assertionID)
        assertionID = 0
        isEnabled = false
    }

    func toggle(reason: String = "Capri-host 正在保持本机唤醒") -> Bool {
        if isEnabled {
            disable()
            return false
        } else {
            return enable(reason: reason)
        }
    }

    deinit {
        disable()
    }
}
