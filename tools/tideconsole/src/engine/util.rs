pub(crate) fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    if unit_index == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.2} {}", UNITS[unit_index])
    }
}

pub(crate) fn format_count(count: usize) -> String {
    const UNITS: &[&str] = &["", "K", "M", "B", "T"];
    let mut size = count as f64;
    let mut unit_index = 0;
    while size >= 1000.0 && unit_index < UNITS.len() - 1 {
        size /= 1000.0;
        unit_index += 1;
    }
    if unit_index == 0 {
        format!("{count}")
    } else {
        format!("{size:.1}{}", UNITS[unit_index])
    }
}
