import SwiftUI

struct SettingsView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(spacing: 0) {
            Form {
                Section("本机") {
                    TextField("显示名", text: $model.hostName)
                    TextField("Host ID", text: $model.hostID)
                    TextField("端口", text: $model.port)
                    Picker("访问范围", selection: $model.bindLAN) {
                        Text("仅本机").tag(false)
                        Text("局域网").tag(true)
                    }
                    .pickerStyle(.segmented)
                    SecureField("本机钥匙", text: $model.feToken)
                    if model.bindLAN {
                        Text("局域网必须填写本机钥匙（FE_TOKEN）。")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                Section("Hub") {
                    TextField("Hub URL", text: $model.hubURL)
                        .onChange(of: model.hubURL) { _ in
                            model.refreshHubTokenState()
                        }
                    if model.hubTokenReady && !model.rePairExpanded {
                        Text(model.hubTokenCaption)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Button("更换配对…") {
                            model.rePairExpanded = true
                        }
                    } else if model.hubTokenReady && model.rePairExpanded {
                        TextField("配对码", text: $model.pairCode)
                        Text("填写新码并保存后，会清掉旧 token 再配对。")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Button("取消") {
                            model.rePairExpanded = false
                            model.pairCode = ""
                        }
                    } else {
                        TextField("配对码", text: $model.pairCode)
                        Text(model.hubTokenCaption)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                Section("Agent") {
                    TextField("grok 路径", text: $model.grokBin, prompt: Text("自动探测"))
                    Button("探测 grok") { model.detectGrok() }
                }
                Section("代理") {
                    TextField("代理地址", text: $model.proxy, prompt: Text("http://127.0.0.1:7890"))
                    TextField("排除地址", text: $model.noProxy, prompt: Text("localhost,127.0.0.1"))
                    Text("留空则沿用系统原有的代理设置；排除地址中的主机直连、不走代理。")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Section("启动") {
                    Toggle("打开应用时启动 Host", isOn: $model.startHostOnLaunch)
                    Toggle("登录时启动 Capri", isOn: $model.startAtLogin)
                }
                Section {
                    LabeledContent("版本", value: capriVersion)
                }
            }
            .formStyle(.grouped)
            Divider()
            HStack(alignment: .center, spacing: 12) {
                Text(model.hint)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
                    .frame(maxWidth: .infinity, alignment: .leading)
                Button("保存") { model.save(andStart: false) }
                    .keyboardShortcut("s", modifiers: .command)
                Button("保存并启动") { model.save(andStart: true) }
                    .keyboardShortcut(.defaultAction)
            }
            .padding(.horizontal, 20)
            .padding(.vertical, 12)
        }
        .frame(minWidth: 460, minHeight: 620)
        .onAppear { model.loadForm() }
    }
}
