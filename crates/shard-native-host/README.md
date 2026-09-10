# shard-native-host

Bridge between a browser extension and the `shard` daemon using WebExtension
[`nativeMessaging`](https://developer.mozilla.org/docs/Mozilla/Add-ons/WebExtensions/Native_messaging).
The browser spawns the host on demand; the host relays messages to the
daemon's Unix control socket and streams `Watch` updates back.

## Install

```sh
cargo build --release -p shard-native-host
./target/release/shard-native-host install --extension my-extension@example.org
```

`install` writes the native-messaging manifests for Firefox
(`~/.mozilla/native-messaging-hosts/`), Chrome and Chromium
(`$XDG_CONFIG_HOME/{google-chrome,chromium}/NativeMessagingHosts/`). Each
manifests points `path` at the host binary. Repeat `--extension` to allow
more extension ids; omit it for a single placeholder id
(`com.shard.native_host`'s default). `--browser firefox|chrome|chromium`
scopes the write.

The manifest-registered binary must be absolute and unchanged after
registration — rebuild in place or re-run `install`.

## Message contract

Transport is native-messaging framing: a `u32` native-endian byte length
followed by exactly one JSON payload. Every payload is a `shard-rpc` value
tagged with `"type"` (snake_case). The host accepts these incoming types:

| type          | fields                                  | daemon reply           |
| ------------- | --------------------------------------- | ---------------------- |
| `ping`        | —                                        | `pong`                 |
| `start`       | `url`, `dest`?, `connections`?          | `started` (id) / error |
| `list`        | —                                        | `list` (downloads)     |
| `status`      | `id` (id or url)                        | `status` (download?)   |
| `pause`       | `id`                                     | `ack` / error          |
| `resume`      | `id`                                     | `ack` / error          |
| `cancel`      | `id`                                     | `ack` / error          |
| `watch`       | —                                        | snapshots (see below)  |

Responses use `pong`, `started`, `list`, `status`, `ack` and `error` types.
An `error` carries `code` and `message`; the host synthesises
`code: "daemon_unreachable"` / `"daemon_error"` when the daemon cannot be
reached or its transport fails.

`watch` switches the channel to streaming: after the request the daemon
pushes a `snapshot` event (`downloads: Vector<DownloadInfo>`) immediately
and on every state change, until the connection closes. When the daemon is
unreachable the host emits a single empty snapshot and the browser should
treat it as "no daemon".

## Daemon lifecycle

The host calls connect with auto-start: if no control socket answers, it
spawns `shard daemon` (override the binary with `$SHARD_DAEMON`), then
polls for up to 5 seconds. Downloads live in the daemon; the browser only
ever talks to the control socket, never to the engine directly.

## Known extensions

See the extension repository (`swirx/shard-extension`) for the manifest id
and the `shard.watch()` / `shard.start(url, opts)` bindings built on this
contract.