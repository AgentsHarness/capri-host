import AppKit
import SwiftUI

@main
struct CapriApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var delegate
    @ObservedObject private var model = AppModel.shared

    var body: some Scene {
        MenuBarExtra {
            Button(model.statusText) {}
                .disabled(true)
            Divider()
            Button("打开界面") { model.openWeb() }
            Button(model.toggleTitle) { model.toggleHost() }
                .disabled(model.toggleDisabled)
            Button("设置…") { SettingsWindow.show() }
                .keyboardShortcut(",", modifiers: .command)
            Toggle("登录时启动", isOn: Binding(
                get: { model.startAtLogin },
                set: { model.setStartAtLogin($0) }
            ))
            Button("打开日志") { model.openLogs() }
            Divider()
            Button("退出 Capri") {
                NSApp.terminate(nil)
            }
        } label: {
            Image(nsImage: menuBarTemplateImage())
        }
        .menuBarExtraStyle(.menu)
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        AppModel.shared.bootstrap()
    }

    func applicationWillTerminate(_ notification: Notification) {
        AppModel.shared.stopHost()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }
}

enum SettingsWindow {
    private static var window: NSWindow?

    static func show() {
        if window == nil {
            let hosting = NSHostingController(rootView: SettingsView(model: AppModel.shared))
            let w = NSWindow(contentViewController: hosting)
            w.title = "Capri 设置"
            w.styleMask = [.titled, .closable, .miniaturizable]
            w.isReleasedWhenClosed = false
            w.setContentSize(NSSize(width: 480, height: 640))
            w.center()
            window = w
        }
        NSApp.activate(ignoringOtherApps: true)
        window?.makeKeyAndOrderFront(nil)
    }
}

func menuBarTemplateImage() -> NSImage {
    let names = ["MenuBarIcon@3x", "MenuBarIcon@2x", "MenuBarIcon"]
    for name in names {
        if let url = Bundle.main.url(forResource: name, withExtension: "png"),
           let img = NSImage(contentsOf: url) {
            img.isTemplate = true
            img.size = NSSize(width: 18, height: 18)
            return img
        }
    }
    let fallback = NSImage(systemSymbolName: "desktopcomputer", accessibilityDescription: "Capri")
        ?? NSImage()
    fallback.isTemplate = true
    return fallback
}
