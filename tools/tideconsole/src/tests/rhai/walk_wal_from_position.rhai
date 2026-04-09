let db = setup_db();
// First pass: collect all entry positions
let positions = [];
db.walk_wal(|entry| { positions.push(entry.position); });
assert(positions.len() >= 2, "need at least 2 entries to test seeking");
let total = positions.len();

// Second pass: resume from the second entry's position — should skip exactly one entry
let resume_pos = positions[1];
let count = 0;
db.walk_wal(resume_pos, |entry| { count += 1; });
assert_eq(count, total - 1, "walking from second entry should skip exactly one entry");
