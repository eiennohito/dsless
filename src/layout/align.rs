use super::resolve::{RenderSpecKind, RenderSpecNode};

// ============================================================
// Cross-table separator alignment
//
// Nested tables at different depths have independently computed column
// widths, so their "│" separators often land 1-2 positions apart.
// Like rivers in typesetting, this is a minor visual defect that becomes
// impossible to unsee once noticed.
//
// Fix: after all tables are resolved, nudge column widths so separators
// line up across visually adjacent tables. Each column can grow by at
// most MAX_ALIGN_ADJUST positions. Optimal alignment across multiple
// tables is found via Viterbi DP — cleaner than special-casing.
// ============================================================

/// Max positions a column can grow to achieve separator alignment.
const MAX_ALIGN_ADJUST: usize = 3;

type NodePath = Vec<usize>;

struct TableEntry {
    path: NodePath,
    info: TableAlignInfo,
}

/// Post-processing pass on the resolved spec tree.
/// Collects all table specs in visual (DFS) order, finds where their
/// separators almost line up, and nudges column widths to close the gap.
pub(super) fn align_adjacent_tables(root: &mut RenderSpecNode) {
    let mut tables = Vec::new();
    collect_tables(root, &mut vec![], &mut tables);

    if tables.len() < 2 {
        return;
    }

    let group = &tables;
    let mut all_adjustments: Vec<(usize, usize, usize)> = Vec::new();

    // Align each separator index independently across all tables that have it
    let max_seps = group
        .iter()
        .map(|t| t.info.sep_positions.len())
        .max()
        .unwrap_or(0);
    for sep_idx in 0..max_seps {
        let participants: Vec<(usize, usize)> = group
            .iter()
            .enumerate()
            .filter_map(|(ti, entry)| entry.info.sep_positions.get(sep_idx).map(|&pos| (ti, pos)))
            .collect();

        if participants.len() < 2 {
            continue;
        }

        for (ti, delta) in viterbi_align(&participants) {
            all_adjustments.push((ti, sep_idx, delta));
        }
    }

    for &(ti, sep_idx, delta) in &all_adjustments {
        let path = &group[ti].path;
        let node = navigate_mut(root, path);
        if let RenderSpecKind::Struct { col_widths, .. } = &mut node.kind
            && sep_idx < col_widths.len()
        {
            col_widths[sep_idx] += delta;
        }
    }
}

/// Walk the spec tree in render order, collecting every table-mode struct.
/// Path records struct child indices only; List/Map are transparent.
fn collect_tables(node: &RenderSpecNode, path: &mut Vec<usize>, out: &mut Vec<TableEntry>) {
    match &node.kind {
        RenderSpecKind::Struct {
            table_mode: true, ..
        } => {
            out.push(TableEntry {
                path: path.clone(),
                info: TableAlignInfo::from_spec(node),
            });
            // Don't recurse into table children — nested tables inside
            // a table cell are a different visual context
        }
        RenderSpecKind::Struct {
            children,
            table_mode: false,
            ..
        } => {
            for (i, child) in children.iter().enumerate() {
                path.push(i);
                collect_tables(&child.spec, path, out);
                path.pop();
            }
        }
        RenderSpecKind::List { element } => {
            // List elements render their table spec for nested tables
            collect_tables(element, path, out);
        }
        RenderSpecKind::Map { key, value } => {
            collect_tables(key, path, out);
            collect_tables(value, path, out);
        }
        _ => {}
    }
}

/// Follow a path to a table-mode struct, descending through List/Map transparently.
fn navigate_mut<'a>(root: &'a mut RenderSpecNode, path: &[usize]) -> &'a mut RenderSpecNode {
    let mut node = root;
    let mut i = 0;
    while i < path.len()
        || matches!(
            &node.kind,
            RenderSpecKind::List { .. } | RenderSpecKind::Map { .. }
        )
    {
        enum Step {
            Child(usize),
            Transparent,
        }
        let step = match &node.kind {
            RenderSpecKind::Struct { .. } if i < path.len() => Step::Child(path[i]),
            RenderSpecKind::List { .. } | RenderSpecKind::Map { .. } => Step::Transparent,
            _ => break,
        };
        match step {
            Step::Child(idx) => {
                let RenderSpecKind::Struct { children, .. } = &mut node.kind else {
                    unreachable!()
                };
                node = &mut children[idx].spec;
                i += 1;
            }
            Step::Transparent => {
                node = match &mut node.kind {
                    RenderSpecKind::List { element } => element.as_mut(),
                    RenderSpecKind::Map { value, .. } => value.as_mut(),
                    _ => unreachable!(),
                };
            }
        }
    }
    node
}

struct TableAlignInfo {
    /// Absolute terminal column of each "│" separator (prefix + cumulative widths).
    /// Two tables align at separator k when sep_positions[k] matches.
    sep_positions: Vec<usize>,
}

impl TableAlignInfo {
    fn from_spec(node: &RenderSpecNode) -> Self {
        let RenderSpecKind::Struct {
            col_widths,
            row_prefix,
            ..
        } = &node.kind
        else {
            return TableAlignInfo {
                sep_positions: vec![],
            };
        };

        let prefix_w = crate::unicode::display_width(row_prefix);
        let mut seps = Vec::with_capacity(col_widths.len().saturating_sub(1));
        let mut cumulative = prefix_w;
        for (i, &cw) in col_widths.iter().enumerate() {
            cumulative += cw;
            if i + 1 < col_widths.len() {
                seps.push(cumulative);
                cumulative += 3; // " │ "
            }
        }

        TableAlignInfo {
            sep_positions: seps,
        }
    }
}

/// Find the column-width adjustments that maximize separator alignment.
///
/// Each table can widen its column by 0..=MAX_ALIGN_ADJUST positions.
/// Viterbi DP over the sequence of tables: state = chosen position,
/// transition cost = 0 if two adjacent tables agree, 1 if they don't.
/// Naturally handles multiple clusters (e.g. tables at 10,12,20,21
/// → two aligned pairs) without special-casing.
fn viterbi_align(participants: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let states: Vec<Vec<usize>> = participants
        .iter()
        .map(|&(_, pos)| (0..=MAX_ALIGN_ADJUST).map(|d| pos + d).collect())
        .collect();

    let n = participants.len();
    if n == 0 {
        return vec![];
    }

    // cost[i][si] = minimum total misalignment cost to reach state si at table i
    // parent[i][si] = which state at table i-1 led to cost[i][si]
    let mut cost: Vec<Vec<usize>> = Vec::with_capacity(n);
    let mut parent: Vec<Vec<usize>> = Vec::with_capacity(n);

    // Initialize first table
    cost.push(vec![0; states[0].len()]);
    parent.push(vec![0; states[0].len()]);

    // Forward pass
    for i in 1..n {
        let mut row_cost = vec![usize::MAX; states[i].len()];
        let mut row_parent = vec![0usize; states[i].len()];

        for (si, &pos_i) in states[i].iter().enumerate() {
            for (sj, &pos_j) in states[i - 1].iter().enumerate() {
                let transition = if pos_i == pos_j { 0 } else { 1 };
                let total = cost[i - 1][sj] + transition;
                if total < row_cost[si] {
                    row_cost[si] = total;
                    row_parent[si] = sj;
                }
            }
        }

        cost.push(row_cost);
        parent.push(row_parent);
    }

    // Backtrack: find best final state
    let last = n - 1;
    let mut best_state = 0;
    for si in 1..states[last].len() {
        if cost[last][si] < cost[last][best_state] {
            best_state = si;
        }
    }

    let mut chosen = vec![0usize; n];
    chosen[last] = best_state;
    for i in (1..n).rev() {
        chosen[i - 1] = parent[i][chosen[i]];
    }

    // Convert to deltas
    let mut result = Vec::new();
    for (i, &(_, orig_pos)) in participants.iter().enumerate() {
        let target_pos = states[i][chosen[i]];
        let delta = target_pos - orig_pos;
        if delta > 0 {
            result.push((i, delta));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_viterbi_all_close() {
        // Three tables at 20, 22, 21 — all within +3 of max
        let participants = vec![(0, 20), (1, 22), (2, 21)];
        let adj = viterbi_align(&participants);
        let target = 22;
        for &(ti, delta) in &adj {
            assert!(delta <= MAX_ALIGN_ADJUST);
            assert_eq!(participants[ti].1 + delta, target);
        }
    }

    #[test]
    fn test_viterbi_snap_up() {
        // Two tables: 17 and 18 — should align to 18
        let participants = vec![(0, 18), (1, 17)];
        let adj = viterbi_align(&participants);
        let mut positions: Vec<usize> = participants.iter().map(|&(_, p)| p).collect();
        for &(ti, delta) in &adj {
            positions[ti] += delta;
        }
        assert_eq!(positions[0], positions[1], "should align");
        assert_eq!(positions[0], 18);
    }

    #[test]
    fn test_viterbi_too_far_apart() {
        // Two tables: 10 and 20 — can't align (delta > 3)
        let participants = vec![(0, 10), (1, 20)];
        let adj = viterbi_align(&participants);
        for &(_, delta) in &adj {
            assert!(delta <= MAX_ALIGN_ADJUST);
        }
    }

    #[test]
    fn test_viterbi_two_clusters() {
        // A=10, B=12, C=20, D=21 — two natural clusters
        let participants = vec![(0, 10), (1, 12), (2, 20), (3, 21)];
        let adj = viterbi_align(&participants);
        let mut positions: Vec<usize> = participants.iter().map(|&(_, p)| p).collect();
        for &(ti, delta) in &adj {
            positions[ti] += delta;
        }
        assert_eq!(positions[0], positions[1], "A and B should align");
        assert_eq!(positions[2], positions[3], "C and D should align");
    }
}
