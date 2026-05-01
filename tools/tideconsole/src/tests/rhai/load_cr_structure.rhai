let db = setup_cr_db();
let cr = db.load_cr();
assert(cr["last_position"] >= 0, "last_position should be non-negative");

let keyspaces = cr["keyspaces"];
assert_eq(keyspaces.len(), 2, "two keyspaces");

let ks0 = keyspaces[0];
assert_eq(ks0["name"], "objects", "first keyspace should be 'objects'");
assert_eq(ks0["id"],   0,         "first keyspace id should be 0");

let cells = ks0["cells"];
assert(cells.len() > 0, "cells array should be non-empty");

let cell = cells[0];
let levels = cell["levels"];
assert(type_of(levels) == "array", "levels should be an array");
for lvl in levels {
    assert(lvl["level"]  >= 0, "level index should be non-negative");
    assert(lvl["offset"] >= 0, "level offset should be non-negative");
    assert(lvl["len"]    >  0, "level len should be positive");
}
assert(cell["last_processed"] >= 0, "last_processed should be non-negative");
