function humanize_time(dt) {
    let days = Math.round(dt / 86400.0);
    let hours = Math.round(dt / 3600.0) % 24;
    let minutes = Math.round(dt / 60.0) % 60;
    let seconds = Math.round(dt) % 60;
    let microseconds = Math.round(((dt % 1.0) * 1000));

    fmtstr = '';
    if(days > 0)
        fmtstr += `${days}d `;
    if(hours > 0)
        fmtstr += `${hours}h `;
    if(minutes > 0)
        fmtstr += `${minutes}m `;
    if(seconds > 0)
        fmtstr += `${seconds}s `;
    if(microseconds > 0)
        // special case: only include ms for times below a minute.
        if(minutes === 0 && hours === 0 && days === 0)
            fmtstr += `${microseconds}ms `;

    return fmtstr.trim();
}