import SwiftUI

struct SettingsView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(spacing: 0) {
            Form {
                Section("本机") {
                    TextField("显示名", text: $model.hostName)
                    TextField("Host ID", text: $model.hostID)
                        .disabled(true)
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
                    if let snap = model.hubSnapshot, snap.configured {
                        if snap.connected {
                            Text("已连接 · \(snap.transport?.uppercased() ?? "在线")")
                                .font(.caption)
                                .foregroundStyle(.green)
                        } else if let err = snap.lastError?.nilIfEmpty {
                            Text("未连接 · \(err)")
                                .font(.caption)
                                .foregroundStyle(.red)
                        } else {
                            Text("连接中…")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }

                    if model.hubTokenReady && !model.rePairExpanded {
                        Text(model.hubTokenCaption)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        HStack {
                            Button("更换配对…") {
                                model.rePairExpanded = true
                            }
                            if model.hubSnapshot?.connected != true && !(model.hubURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty) {
                                Button("立即重连") {
                                    model.onlineReuse()
                                }
                            }
                            if model.hubSnapshot?.configured == true {
                                Button("断开 Hub") {
                                    model.onlineDisconnect()
                                }
                            }
                        }
                    } else if model.hubTokenReady && model.rePairExpanded {
                        TextField("配对码", text: $model.pairCode)
                        Text("保存会清掉旧 token，并把配对码留到下次启动。要现在配对，用「立即配对」。")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        HStack {
                            Button("立即配对") {
                                model.onlinePair()
                            }
                            Button("取消") {
                                model.rePairExpanded = false
                                model.pairCode = ""
                            }
                        }
                    } else {
                        TextField("配对码", text: $model.pairCode)
                        Text(model.hubTokenCaption)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        if !model.pairCode.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            Button("立即配对") {
                                model.onlinePair()
                            }
                        }
                    }
                }
                Section("Agent") {
                    TextField("grok 路径", text: $model.grokBin, prompt: Text("自动探测"))
                    Button("探测 grok") { model.detectGrok() }
                }
                Section("代理") {
                    TextField("代理地址", text: $model.proxy, prompt: Text("http://127.0.0.1:7890"))
                    TextField("排除地址", text: $model.noProxy, prompt: Text("localhost,127.0.0.1"))
                    Text("留空则使用系统网络设置里的代理；系统未开代理时直连。排除地址中的主机不走代理。")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Section("启动") {
                    Toggle("打开应用时启动 Host", isOn: $model.startHostOnLaunch)
                    Toggle("登录时启动 Capri", isOn: $model.startAtLogin)
                    Toggle("阻止电脑休眠", isOn: $model.keepAwake)
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
