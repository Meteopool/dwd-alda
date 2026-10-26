use regex::RegexSet;

/// Passes paths matching any include pattern (or all, if none) and no exclude pattern.
#[derive(Debug, Clone)]
pub struct Filter {
    include: RegexSet,
    exclude: RegexSet,
}

impl Default for Filter {
    fn default() -> Self {
        Filter { include: RegexSet::empty(), exclude: RegexSet::empty() }
    }
}

impl Filter {
    pub fn new<I, E>(include: I, exclude: E) -> Result<Self, regex::Error>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
        E: IntoIterator,
        E::Item: AsRef<str>,
    {
        Ok(Filter { include: RegexSet::new(include)?, exclude: RegexSet::new(exclude)? })
    }

    pub fn is_match(&self, path: &str) -> bool {
        self.check(path).is_some()
    }

    /// Indices of the matching include patterns, `None` if filtered out.
    pub fn check(&self, path: &str) -> Option<Vec<usize>> {
        if self.exclude.is_match(path) {
            return None;
        }
        if self.include.is_empty() {
            return Some(Vec::new());
        }
        let m: Vec<usize> = self.include.matches(path).into_iter().collect();
        (!m.is_empty()).then_some(m)
    }

    pub fn include_patterns(&self) -> &[String] {
        self.include.patterns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn include_exclude() {
        let f = Filter::new([r"^radar/sites/sweep_vol_z/", r"/umd/"], [r"/filter_simple/"]).unwrap();
        assert_eq!(f.check("radar/sites/sweep_vol_z/umd/unfiltered/a"), Some(vec![0, 1]));
        assert_eq!(f.check("radar/sites/sweep_vol_v/umd/unfiltered/a"), Some(vec![1]));
        assert_eq!(f.check("radar/sites/sweep_vol_z/boo/hdf5/filter_simple/a"), None);
        assert_eq!(f.check("nwp/icon/x"), None);
        assert!(Filter::default().is_match("anything"));
    }
}
