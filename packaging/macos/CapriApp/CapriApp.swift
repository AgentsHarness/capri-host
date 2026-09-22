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
            if model.lanBound {
                Button("复制局域网地址") { model.copyLANAddress() }
            }
            if let hubURL = model.hubSnapshot?.hubUrl?.nilIfEmpty, model.hubSnapshot?.connected == true {
                Button("打开 Hub 控制台") {
                    if let u = URL(string: hubURL) { Paths.open(u) }
                }
            }
            Button(model.toggleTitle) { model.toggleHost() }
                .disabled(model.toggleDisabled)
            Button("设置…") { SettingsWindow.show() }
                .keyboardShortcut(",", modifiers: .command)
            Toggle("阻止电脑休眠", isOn: Binding(
                get: { model.keepAwake },
                set: { model.setKeepAwake($0) }
            ))
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

final class SettingsWindow: NSObject, NSWindowDelegate {
    static let shared = SettingsWindow()
    private var window: NSWindow?

    static func show() { shared.present() }

    private func present() {
        if window == nil {
            let hosting = NSHostingController(rootView: SettingsView(model: AppModel.shared))
            let w = NSWindow(contentViewController: hosting)
            w.title = "Capri 设置"
            w.styleMask = [.titled, .closable, .miniaturizable]
            // ARC 下不能把 isReleasedWhenClosed 设为 true。关掉时丢掉引用，
            // 设置页才会释放。
            w.isReleasedWhenClosed = false
            w.delegate = self
            w.setContentSize(NSSize(width: 480, height: 700))
            w.center()
            window = w
        }
        NSApp.activate(ignoringOtherApps: true)
        window?.makeKeyAndOrderFront(nil)
    }

    func windowWillClose(_ notification: Notification) {
        let closing = window
        window = nil
        // 等这次关闭走完再释放，避免 AppKit 还在用这扇窗口。
        DispatchQueue.main.async {
            withExtendedLifetime(closing) {}
        }
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
