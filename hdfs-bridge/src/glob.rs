//! Glob pattern parsing and matching for streaming directory walks.
//!
//! The syntax follows DuckDB's globber (`*`, `?`, `[abc]`, `[a-b]`, `[!abc]`,
//! `\` escapes, and `**` as a whole component matching zero or more levels)
//! extended with Hadoop-style `{a,b}` brace alternation, so patterns from
//! both worlds work. Semantics mirror DuckDB where they differ from Hadoop:
//! `**` must be an entire component (elsewhere consecutive `*`s collapse to
//! one), at most one `**` per pattern, and malformed character classes match
//! nothing rather than erroring.
//!
//! A pattern compiles to a [`GlobPlan`]: brace groups are expanded into a set
//! of component lists, and matching runs as a small NFA over directory levels.
//! The walk carries a set of [`Pos`] states per directory and calls
//! [`GlobPlan::step`] for each child to learn whether to emit it and with
//! which states to descend. This keeps the (async, parallel) walking in
//! `client.rs` and everything pattern-shaped — and unit-testable without a
//! cluster — here.

use hdfs_native::HdfsError;

/// One component (one path level) of an expanded pattern.
#[derive(Debug, PartialEq)]
enum Component {
    /// `**`: matches zero or more path levels.
    Crawl,
    /// No unescaped wildcards; matched by string equality (stored unescaped).
    Literal(String),
    /// Contains wildcards; matched by [`match_component`] (stored raw).
    Pattern(String),
}

/// An NFA state: the walk's current directory has matched components
/// `0..comp` of pattern `pat`, so children are matched against `comp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pos {
    pat: usize,
    comp: usize,
}

/// The result of matching one directory child against a state set.
pub struct Step {
    /// The child is itself a match: emit it.
    pub emit: bool,
    /// States to descend into the child with (empty: don't descend).
    pub next: Vec<Pos>,
}

/// A compiled glob pattern: brace-expanded component lists plus the literal
/// root the walk starts from.
pub struct GlobPlan {
    patterns: Vec<Vec<Component>>,
    root: String,
    initial: Vec<Pos>,
    emit_root: bool,
}

impl GlobPlan {
    pub fn parse(pattern: &str) -> Result<Self, HdfsError> {
        let mut patterns = Vec::new();
        for expanded in expand_braces(pattern)? {
            let components: Vec<Component> = expanded
                .split('/')
                .filter(|c| !c.is_empty())
                .map(parse_component)
                .collect();
            if components
                .iter()
                .filter(|c| matches!(c, Component::Crawl))
                .count()
                > 1
            {
                // Same restriction (and message) as DuckDB's globber.
                return Err(HdfsError::InvalidPath(format!(
                    "Cannot use multiple '**' in one path: '{pattern}'"
                )));
            }
            patterns.push(components);
        }

        // The longest literal prefix shared by all patterns is the walk root:
        // nothing above it needs listing. Patterns keep their full component
        // lists; only the starting index moves.
        let mut prefix: Vec<&str> = Vec::new();
        'grow: loop {
            let k = prefix.len();
            let Some(Component::Literal(first)) = patterns[0].get(k) else {
                break;
            };
            for pattern in &patterns[1..] {
                match pattern.get(k) {
                    Some(Component::Literal(s)) if s == first => {}
                    _ => break 'grow,
                }
            }
            prefix.push(first);
        }
        let consumed = prefix.len();
        let root = format!("/{}", prefix.join("/"));

        // A pattern fully consumed by the prefix (i.e. entirely literal)
        // matches the root itself; the rest start matching at `consumed`.
        let mut initial = Vec::new();
        let mut emit_root = false;
        for (pat, components) in patterns.iter().enumerate() {
            if components.len() == consumed {
                emit_root = true;
            } else {
                initial.push(Pos {
                    pat,
                    comp: consumed,
                });
            }
        }

        Ok(GlobPlan {
            patterns,
            root,
            initial,
            emit_root,
        })
    }

    /// The literal directory (or, for an all-literal pattern, path) the walk
    /// starts from.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// States for the root directory's children. Empty for all-literal
    /// patterns (then [`GlobPlan::emit_root`] is the entire result).
    pub fn initial(&self) -> &[Pos] {
        &self.initial
    }

    /// Whether the root itself is a match (some pattern is fully literal).
    pub fn emit_root(&self) -> bool {
        self.emit_root
    }

    /// Match one child (of a directory holding `states`) by name.
    pub fn step(&self, states: &[Pos], name: &str, is_dir: bool) -> Step {
        let mut step = Step {
            emit: false,
            next: Vec::new(),
        };
        for &pos in states {
            let components = &self.patterns[pos.pat];
            if let Component::Crawl = components[pos.comp] {
                // A crawl consumes the child as an intermediate level...
                if pos.comp + 1 == components.len() {
                    step.emit = true; // terminal `**`: everything matches
                } else {
                    // ...or matches zero levels, deferring to the component
                    // after it.
                    self.match_at(
                        Pos {
                            pat: pos.pat,
                            comp: pos.comp + 1,
                        },
                        name,
                        is_dir,
                        &mut step,
                    );
                }
                if is_dir {
                    push_state(&mut step.next, pos); // keep crawling below
                }
            } else {
                self.match_at(pos, name, is_dir, &mut step);
            }
        }
        step
    }

    /// Match `name` against the single (non-crawl) component at `pos`,
    /// recording an emission or a descent state in `step`.
    fn match_at(&self, pos: Pos, name: &str, is_dir: bool, step: &mut Step) {
        let components = &self.patterns[pos.pat];
        let matched = match &components[pos.comp] {
            Component::Literal(s) => name == s,
            Component::Pattern(p) => match_component(name, p),
            // parse() rejects `**/**`, and step() never advances into a
            // crawl via match_at.
            Component::Crawl => unreachable!("crawl components are handled in step()"),
        };
        if !matched {
            return;
        }
        if pos.comp + 1 == components.len() {
            step.emit = true;
        } else if is_dir {
            push_state(
                &mut step.next,
                Pos {
                    pat: pos.pat,
                    comp: pos.comp + 1,
                },
            );
        }
    }
}

/// Insert a state, keeping the (tiny) set duplicate-free.
fn push_state(states: &mut Vec<Pos>, pos: Pos) {
    if !states.contains(&pos) {
        states.push(pos);
    }
}

/// Classify one raw path component. `**` must be the entire component to act
/// as a crawl (as in DuckDB); anywhere else consecutive `*`s collapse to one.
fn parse_component(comp: &str) -> Component {
    if comp == "**" {
        return Component::Crawl;
    }
    let mut literal = String::with_capacity(comp.len());
    let mut chars = comp.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(next) => literal.push(next),
                None => literal.push('\\'), // dangling escape: keep it literal
            },
            '*' | '?' | '[' => return Component::Pattern(comp.to_string()),
            _ => literal.push(c),
        }
    }
    Component::Literal(literal)
}

/// Expand `{a,b}` brace groups (leftmost-first, recursively, so nesting and
/// groups containing `/` both work) into brace-free patterns. Escaped braces
/// are literal; an unclosed group is an error, a bare `}` is literal.
fn expand_braces(pattern: &str) -> Result<Vec<String>, HdfsError> {
    let chars: Vec<char> = pattern.chars().collect();

    // Find the first unescaped '{'.
    let mut start = None;
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' => i += 1, // skip the escaped character
            '{' => {
                start = Some(i);
                break;
            }
            _ => {}
        }
        i += 1;
    }
    let Some(start) = start else {
        return Ok(vec![pattern.to_string()]);
    };

    // Collect its top-level alternatives up to the matching '}'.
    let mut alternatives = Vec::new();
    let mut current = String::new();
    let mut close = None;
    let mut depth = 1;
    let mut j = start + 1;
    while j < chars.len() {
        let c = chars[j];
        match c {
            '\\' => {
                current.push(c);
                j += 1;
                if j < chars.len() {
                    current.push(chars[j]);
                }
            }
            '{' => {
                depth += 1;
                current.push(c);
            }
            '}' => {
                depth -= 1;
                if depth == 0 {
                    alternatives.push(std::mem::take(&mut current));
                    close = Some(j);
                    break;
                }
                current.push(c);
            }
            ',' if depth == 1 => alternatives.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
        j += 1;
    }
    let Some(close) = close else {
        return Err(HdfsError::InvalidPath(format!(
            "Unclosed brace group in glob pattern '{pattern}'"
        )));
    };

    let prefix: String = chars[..start].iter().collect();
    let suffix: String = chars[close + 1..].iter().collect();
    let mut expanded = Vec::new();
    for alt in alternatives {
        // Recurse to expand nested groups and any further groups in the
        // suffix; each level removes one brace pair, so this terminates.
        expanded.extend(expand_braces(&format!("{prefix}{alt}{suffix}"))?);
    }
    Ok(expanded)
}

/// Match a single path component `name` against `pattern`, DuckDB-style:
/// `*` (any run, consecutive `*`s collapse), `?` (any one char), `[abc]` /
/// `[a-b]` / `[!abc]` character classes, and `\` escaping. Malformed patterns
/// (unclosed class, dangling escape) match nothing.
pub fn match_component(name: &str, pattern: &str) -> bool {
    let name: Vec<char> = name.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    match_chars(&name, &pattern)
}

fn match_chars(s: &[char], p: &[char]) -> bool {
    let mut si = 0;
    let mut pi = 0;
    while si < s.len() && pi < p.len() {
        match p[pi] {
            '*' => {
                while pi < p.len() && p[pi] == '*' {
                    pi += 1;
                }
                if pi == p.len() {
                    return true; // trailing '*' matches the rest
                }
                // Backtrack: try the remaining pattern at every suffix.
                return (si..s.len()).any(|start| match_chars(&s[start..], &p[pi..]));
            }
            '?' => {
                si += 1;
                pi += 1;
            }
            '[' => match match_class(s[si], p, pi + 1) {
                Some(after) => {
                    si += 1;
                    pi = after;
                }
                None => return false,
            },
            '\\' => {
                pi += 1;
                if pi == p.len() || s[si] != p[pi] {
                    return false;
                }
                si += 1;
                pi += 1;
            }
            c => {
                if s[si] != c {
                    return false;
                }
                si += 1;
                pi += 1;
            }
        }
    }
    // Consume trailing '*'s; the match succeeds if both are exhausted.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    si == s.len() && pi == p.len()
}

/// Match `c` against the character class starting at `p[pi]` (just past the
/// `[`). Returns the index after the closing `]` on a match, or `None` when
/// the class doesn't match `c` or is unclosed.
fn match_class(c: char, p: &[char], mut pi: usize) -> Option<usize> {
    let mut invert = false;
    if pi < p.len() && p[pi] == '!' {
        invert = true;
        pi += 1;
    }
    let start = pi;
    let mut found = false;
    loop {
        if pi >= p.len() {
            return None; // unclosed class
        }
        // A ']' in the first position is a literal member.
        if p[pi] == ']' && pi > start {
            pi += 1;
            break;
        }
        // A range "a-b", unless the '-' is the last char before ']'.
        if pi + 2 < p.len() && p[pi + 1] == '-' && p[pi + 2] != ']' {
            if p[pi] <= c && c <= p[pi + 2] {
                found = true;
            }
            pi += 3;
        } else {
            if p[pi] == c {
                found = true;
            }
            pi += 1;
        }
    }
    if found != invert {
        Some(pi)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn component_matching() {
        assert!(match_component("file.txt", "*.txt"));
        assert!(!match_component("file.jpg", "*.txt"));
        assert!(match_component("file.txt", "*"));
        assert!(match_component("", "*"));
        assert!(match_component("abc", "a**c")); // '**' inside a component = '*'
        assert!(match_component("ac", "a*c"));
        assert!(!match_component("ab", "a*c"));

        assert!(match_component("data1.csv", "data?.csv"));
        assert!(!match_component("data12.csv", "data?.csv"));
        assert!(!match_component("data.csv", "data?.csv"));

        assert!(match_component("a.txt", "[abc].txt"));
        assert!(!match_component("d.txt", "[abc].txt"));
        assert!(match_component("file3.txt", "file[0-9].txt"));
        assert!(!match_component("filex.txt", "file[0-9].txt"));
        assert!(match_component("b.txt", "[!a].txt"));
        assert!(!match_component("a.txt", "[!a].txt"));
        assert!(match_component("a-b", "a[x-]b")); // trailing '-' is literal
        assert!(match_component("]", "[]]")); // first ']' is literal

        assert!(match_component("file*name", "file\\*name"));
        assert!(!match_component("fileXname", "file\\*name"));

        // Malformed patterns match nothing (DuckDB behavior), don't error.
        assert!(!match_component("file[", "file["));
        assert!(!match_component("abc", "abc\\"));
    }

    #[test]
    fn brace_expansion() {
        assert_eq!(expand_braces("/a/b").unwrap(), vec!["/a/b"]);
        assert_eq!(expand_braces("/a/{b,c}").unwrap(), vec!["/a/b", "/a/c"]);
        assert_eq!(
            expand_braces("/{a/b,c/d}/e").unwrap(),
            vec!["/a/b/e", "/c/d/e"]
        );
        assert_eq!(expand_braces("/{a,{b,c}}").unwrap(), vec!["/a", "/b", "/c"]);
        assert_eq!(
            expand_braces("/{a,b}/{c,d}").unwrap(),
            vec!["/a/c", "/a/d", "/b/c", "/b/d"]
        );
        // Escaped braces are literal.
        assert_eq!(expand_braces("/a/\\{b,c\\}").unwrap(), vec!["/a/\\{b,c\\}"]);
        // A bare '}' is literal; an unclosed '{' is an error.
        assert_eq!(expand_braces("/a/b}").unwrap(), vec!["/a/b}"]);
        assert!(expand_braces("/a/{b,c").is_err());
    }

    /// Walk `tree` (path -> children as (name, is_dir)) with `plan`,
    /// collecting emitted paths. A sequential stand-in for the real walk.
    fn run_walk(plan: &GlobPlan, tree: &[(&str, Vec<(&str, bool)>)]) -> Vec<String> {
        let children = |path: &str| -> Vec<(String, bool)> {
            tree.iter()
                .find(|(p, _)| *p == path)
                .map(|(_, c)| c.iter().map(|(n, d)| (n.to_string(), *d)).collect())
                .unwrap_or_default()
        };
        let mut emitted = Vec::new();
        let mut queue = vec![(plan.root().to_string(), plan.initial().to_vec())];
        while let Some((path, states)) = queue.pop() {
            for (name, is_dir) in children(&path) {
                let child = if path == "/" {
                    format!("/{name}")
                } else {
                    format!("{path}/{name}")
                };
                let step = plan.step(&states, &name, is_dir);
                if step.emit {
                    emitted.push(child.clone());
                }
                if is_dir && !step.next.is_empty() {
                    queue.push((child, step.next));
                }
            }
        }
        emitted.sort();
        emitted
    }

    #[test]
    fn plan_roots() {
        let plan = GlobPlan::parse("/warehouse/db/table/*.parquet").unwrap();
        assert_eq!(plan.root(), "/warehouse/db/table");
        assert_eq!(plan.initial().len(), 1);
        assert!(!plan.emit_root());

        // Escaped wildcards make an all-literal pattern: the root matches.
        let plan = GlobPlan::parse("/a/file\\*name").unwrap();
        assert_eq!(plan.root(), "/a/file*name");
        assert!(plan.initial().is_empty());
        assert!(plan.emit_root());

        // Brace alternatives share only "/data" -> that's the root.
        let plan = GlobPlan::parse("/data/{2024/01,2025/02}/part-*").unwrap();
        assert_eq!(plan.root(), "/data");
        assert_eq!(plan.initial().len(), 2);

        assert!(GlobPlan::parse("/a/**/b/**").is_err()); // multiple '**'
    }

    #[test]
    fn walk_star_and_classes() {
        let tree = vec![
            (
                "/data",
                vec![("a.csv", false), ("b.txt", false), ("sub", true)],
            ),
            ("/data/sub", vec![("c.csv", false)]),
        ];
        let plan = GlobPlan::parse("/data/*.csv").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/data/a.csv"]);

        // '*' matches directories too, but only emits (never lists) them.
        let plan = GlobPlan::parse("/data/*").unwrap();
        assert_eq!(
            run_walk(&plan, &tree),
            vec!["/data/a.csv", "/data/b.txt", "/data/sub"]
        );

        let plan = GlobPlan::parse("/data/[ab].*").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/data/a.csv", "/data/b.txt"]);

        let plan = GlobPlan::parse("/data/*/*.csv").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/data/sub/c.csv"]);

        let plan = GlobPlan::parse("/data/zzz*").unwrap();
        assert!(run_walk(&plan, &tree).is_empty());
    }

    #[test]
    fn walk_crawl() {
        let tree = vec![
            ("/d", vec![("x.parquet", false), ("a", true)]),
            ("/d/a", vec![("y.parquet", false), ("b", true)]),
            ("/d/a/b", vec![("z.parquet", false)]),
        ];
        // Terminal '**' emits everything at every depth, directories included.
        let plan = GlobPlan::parse("/d/**").unwrap();
        assert_eq!(
            run_walk(&plan, &tree),
            vec![
                "/d/a",
                "/d/a/b",
                "/d/a/b/z.parquet",
                "/d/a/y.parquet",
                "/d/x.parquet"
            ]
        );

        // '**' also matches zero levels (DuckDB semantics).
        let plan = GlobPlan::parse("/d/**/*.parquet").unwrap();
        assert_eq!(
            run_walk(&plan, &tree),
            vec!["/d/a/b/z.parquet", "/d/a/y.parquet", "/d/x.parquet"]
        );

        // A component after '**' prunes what gets emitted, not the descent.
        let plan = GlobPlan::parse("/d/**/b/*.parquet").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/d/a/b/z.parquet"]);
    }

    #[test]
    fn walk_braces() {
        let tree = vec![
            ("/d", vec![("jan", true), ("feb", true), ("mar", true)]),
            ("/d/jan", vec![("1.csv", false)]),
            ("/d/feb", vec![("2.csv", false)]),
            ("/d/mar", vec![("3.csv", false)]),
        ];
        let plan = GlobPlan::parse("/d/{jan,feb}/*.csv").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/d/feb/2.csv", "/d/jan/1.csv"]);

        // Overlapping alternatives must not emit duplicates.
        let plan = GlobPlan::parse("/d/{jan,j*}/*.csv").unwrap();
        assert_eq!(run_walk(&plan, &tree), vec!["/d/jan/1.csv"]);
    }
}
