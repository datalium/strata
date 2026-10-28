//! The arithmetic of the profile: a distribution's percentiles by nearest
//! rank, the same rule the .NET profile uses (`Stats.cs`), so the two engines'
//! P99.9 mean the same thing.

/// The percentiles of a set of samples, in the samples' unit.
#[derive(Debug, Clone, PartialEq)]
pub struct Distribution {
    pub samples: usize,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl Distribution {
    /// Panics if `samples` is empty: a scenario that measured nothing is a bug.
    pub fn of(mut samples: Vec<u64>) -> Self {
        assert!(!samples.is_empty(), "no samples");
        samples.sort_unstable();
        let sum: u128 = samples.iter().map(|&s| s as u128).sum();
        Self {
            samples: samples.len(),
            mean: sum as f64 / samples.len() as f64,
            p50: percentile(&samples, 0.50),
            p90: percentile(&samples, 0.90),
            p99: percentile(&samples, 0.99),
            p999: percentile(&samples, 0.999),
            max: samples[samples.len() - 1],
        }
    }
}

/// The nearest-rank percentile of `sorted`: the smallest sample that at least
/// `fraction` of the samples don't exceed.
pub fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    let rank = (fraction * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_nearest_rank() {
        let sorted: Vec<u64> = (1..=1000).collect();

        assert_eq!(percentile(&sorted, 0.50), 500);
        assert_eq!(percentile(&sorted, 0.90), 900);
        assert_eq!(percentile(&sorted, 0.99), 990);
        assert_eq!(percentile(&sorted, 0.999), 999);
        assert_eq!(percentile(&sorted, 1.0), 1000);
        assert_eq!(percentile(&[7], 0.999), 7);
    }

    #[test]
    fn a_distribution_sorts_its_samples() {
        let distribution = Distribution::of(vec![30, 10, 20, 40]);

        assert_eq!(distribution.samples, 4);
        assert_eq!(distribution.mean, 25.0);
        assert_eq!(distribution.p50, 20);
        assert_eq!(distribution.p999, 40);
        assert_eq!(distribution.max, 40);
    }
}
