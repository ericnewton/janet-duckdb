(declare-project
 :name "duckdb"
 :descriptions "bindings to duckdb")
(declare-native
 :name "duckdb"
 :source @["duckdb.c"]
 :cflags [;default-cflags "-I/opt/homebrew/include" "-g"]
 :ldflags [;default-ldflags "-L/opt/homebrew/lib"  "-lduckdb"]
 )

