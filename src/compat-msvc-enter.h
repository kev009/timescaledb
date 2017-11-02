#ifndef TIMESCALEDB_COMPAT_MSVC_ENTER_H
#define TIMESCALEDB_COMPAT_MSVC_ENTER_H

/*
* Not all exported symbols in PostgreSQL are marked with PGDLLIMPORT, which causes
* errors during linking. This hack turns all extern symbols into properly exported
* symbols so we can use them in our code. Only necessary for files that use these
* incorrectly unlabeled symbols (e.g., extension.c)
*/
#ifdef _MSC_VER
#undef PGDLLIMPORT
#define PGDLLIMPORT
#define extern extern _declspec (dllimport)
#endif /* _MSC_VER */

#endif /* TIMESCALEDB_COMPAT_MSVC_ENTER_H */
