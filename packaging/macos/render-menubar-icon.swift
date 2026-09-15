import AppKit
import Foundation

// Continuous, optically-balanced Capricorn glyph in an 18pt optical box (y-down, 0...18).
func drawCapricorn(in ctx: CGContext, size: CGFloat, lineWidth: CGFloat = 1.45, color: CGColor) {
    let s = size / 18.0
    ctx.saveGState()
    ctx.scaleBy(x: s, y: s)
    ctx.setStrokeColor(color)
    ctx.setLineWidth(lineWidth)
    ctx.setLineCap(.round)
    ctx.setLineJoin(.round)

    // Center optical mass at (9.0, 9.0)
    ctx.translateBy(x: -0.30, y: -0.22)

    let path = CGMutablePath()
    // 1. Top-left horn tip: flare out smoothly
    path.move(to: CGPoint(x: 2.3, y: 3.8))
    // 2. Left downward stroke into the valley (gentle U-curve)
    path.addCurve(to: CGPoint(x: 4.8, y: 11.0),
                  control1: CGPoint(x: 3.5, y: 3.6),
                  control2: CGPoint(x: 3.4, y: 8.2))
    // 3. Ascend smoothly to top peak
    path.addCurve(to: CGPoint(x: 8.2, y: 2.2),
                  control1: CGPoint(x: 6.0, y: 11.0),
                  control2: CGPoint(x: 7.1, y: 4.0))
    // 4. Descend to crossing
    path.addCurve(to: CGPoint(x: 10.6, y: 11.6),
                  control1: CGPoint(x: 9.1, y: 4.0),
                  control2: CGPoint(x: 9.9, y: 9.2))
    // 5. Down-right into bottom curve of loop
    path.addCurve(to: CGPoint(x: 13.8, y: 14.3),
                  control1: CGPoint(x: 11.2, y: 13.4),
                  control2: CGPoint(x: 12.2, y: 14.3))
    // 6. Loop up-right
    path.addCurve(to: CGPoint(x: 16.1, y: 10.5),
                  control1: CGPoint(x: 15.3, y: 14.3),
                  control2: CGPoint(x: 16.3, y: 12.6))
    // 7. Loop top arch
    path.addCurve(to: CGPoint(x: 13.5, y: 7.0),
                  control1: CGPoint(x: 15.9, y: 8.4),
                  control2: CGPoint(x: 14.8, y: 7.0))
    // 8. Loop descending left through crossing
    path.addCurve(to: CGPoint(x: 10.6, y: 11.6),
                  control1: CGPoint(x: 12.1, y: 7.0),
                  control2: CGPoint(x: 11.3, y: 9.4))
    // 9. Sweep gracefully through crossing into tail
    path.addCurve(to: CGPoint(x: 5.3, y: 15.6),
                  control1: CGPoint(x: 9.6, y: 14.2),
                  control2: CGPoint(x: 7.2, y: 15.9))

    ctx.addPath(path)
    ctx.strokePath()
    ctx.restoreGState()
}

func drawAppIcon(in ctx: CGContext, size: CGFloat) {
    let rect = CGRect(x: 0, y: 0, width: size, height: size)
    let cornerRadius = size * 0.2237 // Apple standard macOS squircle corner radius ratio
    let squirclePath = CGPath(roundedRect: rect, cornerWidth: cornerRadius, cornerHeight: cornerRadius, transform: nil)

    ctx.saveGState()
    ctx.addPath(squirclePath)
    ctx.clip()

    // 1. Cosmic diagonal gradient: Dark Indigo (#101C44) -> Deep Space (#090E26) -> Cosmic Violet (#160C2A)
    let colorSpace = CGColorSpaceCreateDeviceRGB()
    let cTopLeft = CGColor(red: 16 / 255.0, green: 28 / 255.0, blue: 68 / 255.0, alpha: 1.0)
    let cMid = CGColor(red: 9 / 255.0, green: 14 / 255.0, blue: 38 / 255.0, alpha: 1.0)
    let cBottomRight = CGColor(red: 22 / 255.0, green: 12 / 255.0, blue: 42 / 255.0, alpha: 1.0)
    let gradient = CGGradient(colorsSpace: colorSpace,
                              colors: [cTopLeft, cMid, cBottomRight] as CFArray,
                              locations: [0.0, 0.55, 1.0])!
    ctx.drawLinearGradient(gradient,
                           start: CGPoint(x: 0, y: 0),
                           end: CGPoint(x: size, y: size),
                           options: [])

    // 2. Soft Radial Nebula Glow in upper-center (cyan-blue starlight bloom)
    let cGlow = CGColor(red: 60 / 255.0, green: 120 / 255.0, blue: 220 / 255.0, alpha: 0.22)
    let cClear = CGColor(red: 0, green: 0, blue: 0, alpha: 0.0)
    let radialGrad = CGGradient(colorsSpace: colorSpace,
                                colors: [cGlow, cClear] as CFArray,
                                locations: [0.0, 1.0])!
    ctx.drawRadialGradient(radialGrad,
                           startCenter: CGPoint(x: size * 0.45, y: size * 0.35),
                           startRadius: 0,
                           endCenter: CGPoint(x: size * 0.45, y: size * 0.35),
                           endRadius: size * 0.65,
                           options: [])

    // 3. Stardust points (Capricorn constellation subtle stars)
    let starScale = size / 512.0
    let stars: [(CGFloat, CGFloat, CGFloat, CGFloat)] = [
        (0.22, 0.18, 1.8, 0.65), // Alpha / Prima Giedi
        (0.28, 0.16, 1.4, 0.50), // Secunda Giedi
        (0.35, 0.25, 2.2, 0.85), // Dabih (Beta Cap) - bright
        (0.72, 0.28, 1.6, 0.55),
        (0.82, 0.52, 2.0, 0.80), // Deneb Algedi (Delta Cap)
        (0.78, 0.68, 1.5, 0.60), // Nashira (Gamma Cap)
        (0.48, 0.85, 1.8, 0.50), // Omega Cap
        (0.18, 0.72, 1.3, 0.40),
        (0.60, 0.15, 1.0, 0.35),
        (0.85, 0.20, 1.2, 0.40),
        (0.15, 0.45, 1.1, 0.35)
    ]
    for (rx, ry, r, alpha) in stars {
        let pt = CGPoint(x: size * rx, y: size * ry)
        let scaledR = r * starScale
        // Faint starlight halo
        ctx.setFillColor(CGColor(red: 180 / 255.0, green: 220 / 255.0, blue: 255 / 255.0, alpha: alpha * 0.3))
        ctx.fillEllipse(in: CGRect(x: pt.x - scaledR * 2.2, y: pt.y - scaledR * 2.2, width: scaledR * 4.4, height: scaledR * 4.4))
        // Star core
        ctx.setFillColor(CGColor(gray: 1.0, alpha: alpha))
        ctx.fillEllipse(in: CGRect(x: pt.x - scaledR * 0.6, y: pt.y - scaledR * 0.6, width: scaledR * 1.2, height: scaledR * 1.2))
    }

    // 4. Subtle celestial rim light
    ctx.setStrokeColor(CGColor(red: 160 / 255.0, green: 200 / 255.0, blue: 255 / 255.0, alpha: 0.18))
    ctx.setLineWidth(size * 0.008)
    ctx.addPath(squirclePath)
    ctx.strokePath()

    // 5. White Glyph with celestial outer glow
    let inset = size * 0.15
    let glyphSize = size - 2 * inset
    ctx.saveGState()
    ctx.translateBy(x: inset, y: inset)
    ctx.setShadow(offset: .zero, blur: size * 0.03, color: CGColor(red: 100 / 255.0, green: 180 / 255.0, blue: 255 / 255.0, alpha: 0.45))
    drawCapricorn(in: ctx, size: glyphSize, lineWidth: 1.15, color: CGColor(gray: 1.0, alpha: 1.0))
    ctx.restoreGState()

    ctx.restoreGState()
}

func makeContext(pixel: Int) -> CGContext {
    let colorSpace = CGColorSpaceCreateDeviceRGB()
    let bitmap = CGContext(
        data: nil,
        width: pixel,
        height: pixel,
        bitsPerComponent: 8,
        bytesPerRow: 0,
        space: colorSpace,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    )!
    bitmap.translateBy(x: 0, y: CGFloat(pixel))
    bitmap.scaleBy(x: 1, y: -1)
    return bitmap
}

func renderMenuBar(pixel: Int, preview: Bool) -> NSBitmapImageRep {
    let bitmap = makeContext(pixel: pixel)
    if preview {
        bitmap.setFillColor(CGColor(gray: 1, alpha: 1))
        bitmap.fill(CGRect(x: 0, y: 0, width: pixel, height: pixel))
    } else {
        bitmap.clear(CGRect(x: 0, y: 0, width: pixel, height: pixel))
    }
    drawCapricorn(in: bitmap, size: CGFloat(pixel), lineWidth: 1.45, color: CGColor(gray: 0, alpha: 1))
    let cg = bitmap.makeImage()!
    return NSBitmapImageRep(cgImage: cg)
}

func renderAppIcon(pixel: Int) -> NSBitmapImageRep {
    let bitmap = makeContext(pixel: pixel)
    drawAppIcon(in: bitmap, size: CGFloat(pixel))
    let cg = bitmap.makeImage()!
    return NSBitmapImageRep(cgImage: cg)
}

func writePNG(_ rep: NSBitmapImageRep, to path: String) {
    let data = rep.representation(using: .png, properties: [:])!
    try! data.write(to: URL(fileURLWithPath: path))
}

let out = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "."
writePNG(renderMenuBar(pixel: 18, preview: false), to: "\(out)/MenuBarIcon.png")
writePNG(renderMenuBar(pixel: 36, preview: false), to: "\(out)/MenuBarIcon@2x.png")
writePNG(renderMenuBar(pixel: 54, preview: false), to: "\(out)/MenuBarIcon@3x.png")
writePNG(renderAppIcon(pixel: 1024), to: "\(out)/AppIcon.png")
writePNG(renderMenuBar(pixel: 256, preview: true), to: "/tmp/menubar-even-preview.png")
writePNG(renderAppIcon(pixel: 512), to: "/tmp/appicon-preview.png")
print("wrote icons to \(out)")
