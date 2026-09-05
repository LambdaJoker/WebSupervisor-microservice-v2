use tao::{
    dpi::LogicalSize,
    event::{Event, WindowEvent},
    event_loop::{ControlFlow, EventLoop},
    window::{Icon, WindowBuilder},
};
use wry::WebViewBuilder;

use super::{shutdown_services, AppState};

fn desktop_icon() -> Option<Icon> {
    const SIZE: u32 = 64;
    let mut pixels = vec![0u8; (SIZE * SIZE * 4) as usize];

    for y in 0..SIZE {
        for x in 0..SIZE {
            let edge = 5.0f32;
            let radius = 15.0f32;
            let px = x as f32 + 0.5;
            let py = y as f32 + 0.5;
            let cx = px.clamp(edge + radius, SIZE as f32 - edge - radius);
            let cy = py.clamp(edge + radius, SIZE as f32 - edge - radius);
            let corner_distance = ((px - cx).powi(2) + (py - cy).powi(2)).sqrt();
            let mut alpha = if corner_distance <= radius {
                255.0
            } else {
                0.0
            };
            if corner_distance > radius - 1.0 && corner_distance <= radius {
                alpha = (radius - corner_distance) * 255.0;
            }

            let t = (px + py) / (SIZE as f32 * 2.0);
            let r = (13.0 + 90.0 * t) as u8;
            let g = (107.0 + 108.0 * t) as u8;
            let b = (255.0 - 35.0 * t) as u8;
            let index = ((y * SIZE + x) * 4) as usize;
            pixels[index] = r;
            pixels[index + 1] = g;
            pixels[index + 2] = b;
            pixels[index + 3] = alpha.clamp(0.0, 255.0) as u8;
        }
    }

    let paths = [
        ((14.0, 21.0), (21.0, 45.0)),
        ((21.0, 45.0), (32.0, 21.0)),
        ((32.0, 21.0), (43.0, 45.0)),
        ((43.0, 45.0), (50.0, 21.0)),
    ];
    for y in 0..SIZE {
        for x in 0..SIZE {
            let px = x as f32 + 0.5;
            let py = y as f32 + 0.5;
            let on_mark = paths
                .iter()
                .any(|&(start, end)| point_segment_distance(px, py, start, end) <= 3.1);
            let on_dot = (px - 50.0).powi(2) + (py - 13.0).powi(2) <= 4.0f32.powi(2);
            if on_mark || on_dot {
                let index = ((y * SIZE + x) * 4) as usize;
                if on_dot {
                    pixels[index] = 255;
                    pixels[index + 1] = 207;
                    pixels[index + 2] = 112;
                } else {
                    pixels[index] = 255;
                    pixels[index + 1] = 255;
                    pixels[index + 2] = 255;
                }
                pixels[index + 3] = 255;
            }
        }
    }

    Icon::from_rgba(pixels, SIZE, SIZE).ok()
}

fn point_segment_distance(px: f32, py: f32, start: (f32, f32), end: (f32, f32)) -> f32 {
    let (sx, sy) = start;
    let (ex, ey) = end;
    let dx = ex - sx;
    let dy = ey - sy;
    let length_squared = dx * dx + dy * dy;
    let projection = if length_squared == 0.0 {
        0.0
    } else {
        ((px - sx) * dx + (py - sy) * dy) / length_squared
    };
    let t = projection.clamp(0.0, 1.0);
    let nearest_x = sx + t * dx;
    let nearest_y = sy + t * dy;
    ((px - nearest_x).powi(2) + (py - nearest_y).powi(2)).sqrt()
}

/// Native desktop shell backed by the existing web UI.
///
/// Wry keeps the binary small while letting the product UI use the same HTML/CSS
/// in the browser and in the desktop window. All service logic remains in the
/// Rust HTTP API; this module only owns the native window.
pub fn run(state: AppState) -> Result<(), String> {
    let event_loop = EventLoop::new();
    let window = WindowBuilder::new()
        .with_title("WebSupervisor")
        .with_window_icon(desktop_icon())
        .with_inner_size(LogicalSize::new(1440.0, 920.0))
        .with_min_inner_size(LogicalSize::new(980.0, 640.0))
        .build(&event_loop)
        .map_err(|error| format!("创建桌面窗口失败: {error}"))?;

    let url = format!("http://{}", state.config.listen_addr);
    let builder = WebViewBuilder::new()
        .with_url(&url)
        .with_new_window_req_handler(|_, _| wry::NewWindowResponse::Deny);

    #[cfg(any(
        target_os = "windows",
        target_os = "macos",
        target_os = "ios",
        target_os = "android"
    ))]
    let _webview = builder
        .build(&window)
        .map_err(|error| format!("创建桌面 WebView 失败: {error}"))?;

    #[cfg(not(any(
        target_os = "windows",
        target_os = "macos",
        target_os = "ios",
        target_os = "android"
    )))]
    let _webview = {
        use tao::platform::unix::WindowExtUnix;
        use wry::WebViewBuilderExtUnix;
        let vbox = window
            .default_vbox()
            .ok_or_else(|| "Linux 桌面窗口未提供默认容器".to_string())?;
        builder
            .build_gtk(vbox)
            .map_err(|error| format!("创建桌面 WebView 失败: {error}"))?
    };

    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;
        if let Event::WindowEvent {
            event: WindowEvent::CloseRequested,
            ..
        } = event
        {
            shutdown_services(&state);
            *control_flow = ControlFlow::Exit;
        }
    });
}
