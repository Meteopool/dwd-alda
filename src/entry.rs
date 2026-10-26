use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Placement time from alda.log, 1 s resolution.
    pub time: SystemTime,
    pub size: u64,
    /// Relative to the alda.log directory.
    pub path: String,
    pub seen: SystemTime,
    /// Indices of the include patterns that matched.
    pub matched: Vec<usize>,
}

impl Entry {
    pub fn url(&self, base: &str) -> String {
        format!("{base}{}", self.path)
    }

    pub fn sweep(&self) -> Option<Sweep> {
        Sweep::parse(&self.path)
    }

    pub(crate) fn parse_line(line: &str, seen: SystemTime) -> Option<Entry> {
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let mut it = line.splitn(3, '|');
        let t: u64 = it.next()?.parse().ok()?;
        let size: u64 = it.next()?.parse().unwrap_or(0);
        let path = it.next()?;
        Some(Entry {
            time: UNIX_EPOCH + Duration::from_secs(t),
            size,
            path: path.to_owned(),
            seen,
            matched: Vec::new(),
        })
    }
}

/// Metadata from a `radar/sites/sweep_*` file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sweep {
    pub product: String,
    pub site: String,
    pub subdir: String,
    pub moment: String,
    pub index: u8,
    pub scan: SystemTime,
}

static SWEEP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"radar/sites/(sweep_\w+)/(\w+)/(.+)/[^/]*_([a-z]+)_(\d\d)-(\d{14})\d*-\w+-\d+-hd5$").unwrap()
});

impl Sweep {
    pub fn parse(path: &str) -> Option<Sweep> {
        let c = SWEEP_RE.captures(path)?;
        Some(Sweep {
            product: c[1].to_owned(),
            site: c[2].to_owned(),
            subdir: c[3].to_owned(),
            moment: c[4].to_owned(),
            index: c[5].parse().ok()?,
            scan: parse_utc(&c[6])?,
        })
    }
}

// Hand-rolled (Hinnant's days_from_civil) to avoid a date crate for one format.
fn parse_utc(s: &str) -> Option<SystemTime> {
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0..4)?, n(4..6)?, n(6..8)?);
    let (h, mi, se) = (n(8..10)?, n(10..12)?, n(12..14)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let y = if mo <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days * 86400 + h * 3600 + mi * 60 + se;
    u64::try_from(secs).ok().map(|s| UNIX_EPOCH + Duration::from_secs(s))
}

pub fn unix_secs(t: SystemTime) -> f64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_from_path() {
        let s = Sweep::parse("radar/sites/sweep_vol_z/boo/hdf5/filter_simple/ras07-stqual-vol5minng01_sweeph5onem_dbzh_07-2026102200005800-boo-10132-hd5").unwrap();
        assert_eq!(s.product, "sweep_vol_z");
        assert_eq!(s.site, "boo");
        assert_eq!(s.subdir, "hdf5/filter_simple");
        assert_eq!(s.moment, "dbzh");
        assert_eq!(s.index, 7);
        assert_eq!(unix_secs(s.scan), 1_792_627_258.0);
        assert!(
            Sweep::parse(
                "radar/sites/sweep_vol_z/umd/unfiltered/ras07-vol5minng01_sweeph5onem_th_00-LATEST-umd-10356-hd5"
            )
            .is_none()
        );
    }

    #[test]
    fn line_parsing() {
        let e = Entry::parse_line("1792627203|6812|weather_reports/x.bin", UNIX_EPOCH).unwrap();
        assert_eq!((unix_secs(e.time), e.size, e.path.as_str()), (1_792_627_203.0, 6812, "weather_reports/x.bin"));
        assert!(Entry::parse_line("# V1.0 inode=1 node=2", UNIX_EPOCH).is_none());
        assert!(Entry::parse_line("garbage", UNIX_EPOCH).is_none());
    }
}
