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
let offset = cell["offset"];
assert(offset == -1 || offset >= 0, "offset must be -1 (invalid) or >=0");
assert(cell["len"]            >= 0, "len should be non-negative");
assert(cell["last_processed"] >= 0, "last_processed should be non-negative");
