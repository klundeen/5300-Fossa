# 5300-Fossa

Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Usage

``` sh
./sql5300 DB
```

Options:

- `DB`: Path to database

## Set Up <a name="setup"></a>

### Dependencies

Building requires the following libraries:

- [SQL Parser for Hyrise (fork)](https://github.com/klundeen/sql-parser)
- [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html) (tested on 6.2.32, 18.1.38)

Running the tests additionally requires:

- [GoogleTest](https://google.github.io/googletest/)

### Building <a name="building"></a>

Building can be accomplished by running gnumake

``` sh
make
```

### Testing

Unit tests can be built and run with:

``` sh
make check
```

NOTE: CS1 is missing the headers for gmock and will thus fail to build. I have included the headers in this repo but due to CS1's version of gmock being old they are not built with by default. Use the following to patch the makefile:

``` sh
# Apply patch
git apply 0001-cs1-gmock-add-headers.patch
# Run tests
make check
```

The builtin action tester can be run from the sql shell:

``` sh
$ ./sql5300 $datadir
SQL> test
```

## Tags

- `Milestone1`: Initial parser support that simply prints back parsed SQL syntax.
