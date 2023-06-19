(import duckdb)
(import spork/test)

# trivial module call
(assert (string/has-prefix? "v" (duckdb/library_version)) "unexpected version")

# basic open/close
(:close (duckdb/open))

# open with path
(var db (duckdb/open ".test.db"))

# connect/disconnect
(var conn (:connect db))
(:disconnect conn)

# alternate name for disconnect
(var conn (:connect db))
(:close conn)

(:close db)

(defn exists? (path)
  (not (nil? (os/stat path))))

# verify we used a path
(assert (exists? ".test.db") "ensure db file exists")
(os/rm ".test.db")

(var db (duckdb/open))
(var conn (:connect db))
(:eval conn "create table test(i8 tinyint, i16 smallint, i32 integer, i64 bigint, s varchar, b blob);")

(test/assert-error "expected bad bind type"
		   (:eval conn "select 1 where ?" :a))
(test/assert-error "bad type: can't (yet) bind to a composite"
		   (:eval conn "select 1 where ?" [1 2 3]))
(test/assert-error "too few params"
		   (:eval conn "select i8 from test where i32 = ?"))
(test/assert-error "too many params"
		   (:eval conn "select i8 from test where i32 = ?" 1 2))

# binding for types going in
(:eval conn "insert into test(i8, i16, i32, i64, s, b) values (?, ?, ?, ?, ?, ?)" 1 2 3 4 "s" @"b")
# binding for results coming out
#  note: buffers do not compare for equality (assert @"b" @"b") fails, so make strings from buffers
(var v (:eval conn "select i8, i16, i32, i64, s, b::VARCHAR as b from test where i8 = ?" 1))
(var row (get v 0))
(assert (= {:i8 1 :i16 2 :i32 3 :i64 4 :s "s" :b "b"} row))

# read out one buffer object for testing
(def v (:eval conn "select b from test where i8 = ?" 1))
(def row (get v 0))
(def b (get row :b))
(assert (= "b" (string b)))

(:close conn)
(:close db)

# test function entry points
(var db (duckdb/open))
(var conn (duckdb/connect db))
(var v (duckdb/eval conn "select 1 as a where ?" true))
(var row (get v 0))
(assert (= {:a 1} row))
(duckdb/disconnect conn)
(duckdb/close db)
