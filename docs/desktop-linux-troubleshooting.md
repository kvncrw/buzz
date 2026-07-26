# Desktop app on Linux: graphics troubleshooting

The desktop app renders through the system WebKitGTK webview (Tauri). On some Linux GPU/driver/compositor combinations — reported most often on rolling-release distros (Arch, Omarchy, Garuda) under Wayland, on both AMD and NVIDIA — WebKitGTK's GPU rendering paths fail and the app crashes at launch or opens a window that never renders. WebKitGTK honors environment variables that select safer rendering paths; no rebuild is needed.

Set the variable when launching the app, for example:

```sh
WEBKIT_DISABLE_DMABUF_RENDERER=1 ./Buzz_amd64.AppImage
```

To make it permanent, export it from your shell profile or add it to the `Exec=` line of your `.desktop` entry (`env WEBKIT_DISABLE_DMABUF_RENDERER=1 buzz-desktop`).

## Which variable to try

Work down this list in order; stop at the first one that fixes the problem. Each successive option disables more GPU acceleration, so don't set them preemptively.

| Symptom | Variable | Effect |
|---|---|---|
| App starts and prints its log lines, then the WebKitWebProcess core-dumps and no window appears ([#2338](https://github.com/block/buzz/issues/2338)) | `WEBKIT_DISABLE_DMABUF_RENDERER=1` | Disables WebKitGTK's DMA-BUF renderer in favor of shared-memory buffers. The most common fix; try it first. |
| Still crashing or blank after the above; crashes when resizing the window | `WEBKIT_DISABLE_COMPOSITING_MODE=1` | Disables accelerated compositing entirely. Heavier fallback — only use it if the DMA-BUF variable is not enough. |
| Window opens but its contents are invisible/transparent on AMD RDNA4 with the radv driver, and the DMA-BUF variable does not help ([#2643](https://github.com/block/buzz/issues/2643)) | `WEBKIT_SKIA_ENABLE_CPU_RENDERING=1` | Forces WebKitGTK's Skia backend to render on the CPU. |

These are upstream WebKitGTK behaviors that affect all apps embedding it; Tauri documents the same variables in its [Linux graphics debugging guide](https://v2.tauri.app/develop/debug/linux-graphics/).

## AppImage launch failures on older releases

Releases built from current `main` already repair several AppImage-only launch crashes on newer distros — the bundled `libsystemd` version mismatch (`libsystemd.so.0: version 'LIBSYSTEMD_251' not found`, [#2335](https://github.com/block/buzz/issues/2335)), the GStreamer plugin-path override that blanked the window, and the bundled Wayland/EGL library skew — via `desktop/scripts/fix-appimage.sh`, which CI applies to release AppImages. If you hit those errors, upgrade to the latest release first; on a current build only the WebKitGTK variables above should still be relevant.
