(declare-project
 :name "duckdb"
 :descriptions "bindings to duckdb")
(def duckdb-home (os/getenv "DUCKDB_DEV_HOME" "/opt/homebrew"))
(declare-native
 :name "duckdb"
 :source @["duckdb.c"]
 :cflags [;default-cflags (string/format "-I%s/include" duckdb-home) "-g"]
 :ldflags [;default-ldflags (string/format "-L%s/lib" duckdb-home)  "-lduckdb"]
 )

