use super::*;

impl Compactor {
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        finalize_pending_compaction_replacements(&self.data_path)?;
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L0,
            CompactionLevel::L1,
            self.l0_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L1,
            CompactionLevel::L2,
            self.l1_trigger,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) =
            self.try_merge_small_segments(CompactionLevel::L2, self.l1_trigger, &tombstones)?
        {
            return Ok(outcome);
        }

        Ok(CompactionOutcome::default())
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let source_level = level_to_u8(source);

        // Planning only needs manifests. Loading every segment of the level
        // with its chunks here made each pass read the whole level into
        // memory, which grows without bound while compaction lags behind.
        let mut candidates = scan_level_for_compaction(&self.data_path, source_level)?;
        if candidates.len() < 2 {
            return Ok(None);
        }

        candidates.sort_by_key(|candidate| candidate.manifest.segment_id);

        let should_compact = candidates.len() >= count_trigger || has_time_overlap(&candidates);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&candidates, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS);
        self.compact_window(source, target, &window, tombstones)
    }

    /// Merges a run of small segments within the last level into one.
    ///
    /// Nothing compacts the last level further, so without this every level-1
    /// compaction leaves another small segment behind and the number of
    /// segments grows with the age of the data.
    fn try_merge_small_segments(
        &self,
        level: CompactionLevel,
        count_trigger: usize,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let mut candidates = scan_level_for_compaction(&self.data_path, level_to_u8(level))?;
        if candidates.len() < count_trigger.max(2) {
            return Ok(None);
        }

        candidates.sort_by_key(|candidate| {
            (
                candidate.manifest.min_ts.unwrap_or(i64::MIN),
                candidate.manifest.segment_id,
            )
        });
        let window = select_small_segment_window(
            &candidates,
            count_trigger,
            DEFAULT_SOURCE_WINDOW_SEGMENTS,
            self.output_segment_point_budget(),
        );
        self.compact_window(level, level, &window, tombstones)
    }

    fn compact_window(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        window: &[&CompactionCandidate],
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        if window.len() < 2 {
            return Ok(None);
        }
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        let roots = window
            .iter()
            .map(|candidate| candidate.root.clone())
            .collect::<Vec<_>>();
        let loaded = load_segment_dirs_runtime_strict(&roots)?;
        if loaded.len() != roots.len() {
            // A segment disappeared since the scan; plan again next pass.
            return Ok(None);
        }
        let window = loaded.iter().collect::<Vec<_>>();

        let mut outcome = self.compact_segments(target_level, &window, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        Ok(Some(outcome))
    }
}

/// A segment considered for compaction, known by its manifest only.
pub(super) struct CompactionCandidate {
    root: PathBuf,
    manifest: SegmentManifest,
}

pub(super) fn scan_level_for_compaction(
    base: &Path,
    level: u8,
) -> Result<Vec<CompactionCandidate>> {
    let expected_level_dir = format!("L{level}");
    let mut candidates = Vec::new();

    for root in list_segment_dirs(base)? {
        if root
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(expected_level_dir.as_str())
        {
            continue;
        }

        match read_segment_manifest(&root) {
            Ok(manifest) => {
                if manifest.level != level {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &format!(
                            "segment directory level mismatch: stored under L{level}, manifest says L{}",
                            manifest.level
                        ),
                    ));
                }
                candidates.push(CompactionCandidate { root, manifest });
            }
            Err(err) if is_not_found_error(&err) => continue,
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(candidates)
}

pub(super) fn has_time_overlap(segments: &[CompactionCandidate]) -> bool {
    let mut ranges = segments
        .iter()
        .filter_map(|segment| {
            Some((
                segment.manifest.min_ts?,
                segment.manifest.max_ts?,
                segment.manifest.segment_id,
            ))
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return false;
    }

    ranges.sort_by_key(|(min_ts, _, segment_id)| (*min_ts, *segment_id));

    let mut current_max = ranges[0].1;
    for (min_ts, max_ts, _) in ranges.into_iter().skip(1) {
        if min_ts <= current_max {
            return true;
        }
        current_max = current_max.max(max_ts);
    }

    false
}

/// Returns the oldest run of at least `count_trigger` adjacent segments that
/// are each smaller than half an output segment, cut to at most `max_segments`
/// and to what fits into one output segment. Merging only small segments keeps
/// rewriting the same data to a few times over its lifetime.
pub(super) fn select_small_segment_window(
    segments: &[CompactionCandidate],
    count_trigger: usize,
    max_segments: usize,
    output_point_budget: usize,
) -> Vec<&CompactionCandidate> {
    let is_small =
        |segment: &CompactionCandidate| segment.manifest.point_count < output_point_budget / 2;
    let min_run = count_trigger.max(2);

    let mut start = 0usize;
    while start < segments.len() {
        let run_len = segments[start..]
            .iter()
            .take_while(|segment| is_small(segment))
            .count();
        if run_len >= min_run {
            let mut points = 0usize;
            return segments[start..start + run_len]
                .iter()
                .take(max_segments.max(2))
                .take_while(|segment| {
                    points = points.saturating_add(segment.manifest.point_count);
                    points <= output_point_budget
                })
                .collect();
        }
        start += run_len.max(1);
    }

    Vec::new()
}

pub(super) fn select_compaction_window(
    segments: &[CompactionCandidate],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&CompactionCandidate> {
    let max_segments = max_segments.max(2);
    if let Some(indexes) = overlapping_window_indexes(segments, max_segments) {
        return indexes
            .into_iter()
            .filter_map(|index| segments.get(index))
            .collect();
    }

    let window_len = count_trigger.max(2).min(max_segments).min(segments.len());
    segments.iter().take(window_len).collect()
}

#[derive(Debug, Clone, Copy)]
struct SegmentTimeRange {
    index: usize,
    segment_id: u64,
    min_ts: i64,
    max_ts: i64,
}

fn overlapping_window_indexes(
    segments: &[CompactionCandidate],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut ranges = segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| {
            Some(SegmentTimeRange {
                index,
                segment_id: segment.manifest.segment_id,
                min_ts: segment.manifest.min_ts?,
                max_ts: segment.manifest.max_ts?,
            })
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return None;
    }

    ranges.sort_by_key(|range| (range.min_ts, range.segment_id));

    let mut cluster_start = 0usize;
    let mut cluster_max = ranges[0].max_ts;

    for idx in 1..ranges.len() {
        if ranges[idx].min_ts <= cluster_max {
            cluster_max = cluster_max.max(ranges[idx].max_ts);
            continue;
        }

        if idx.saturating_sub(cluster_start) >= 2 {
            return Some(select_cluster_indexes(
                &ranges[cluster_start..idx],
                max_segments,
            ));
        }

        cluster_start = idx;
        cluster_max = ranges[idx].max_ts;
    }

    if ranges.len().saturating_sub(cluster_start) >= 2 {
        return Some(select_cluster_indexes(
            &ranges[cluster_start..],
            max_segments,
        ));
    }

    None
}

fn select_cluster_indexes(cluster: &[SegmentTimeRange], max_segments: usize) -> Vec<usize> {
    let mut indexes = cluster.iter().map(|range| range.index).collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.truncate(max_segments.max(2));
    indexes
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
