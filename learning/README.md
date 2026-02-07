# Python + Odibi Learning Program

## Welcome — What This Is and Why It Exists

You built Odibi. A real, working data engineering framework used in production. You understand
the concepts — pipelines, SCD2, data quality, engine abstraction, declarative configuration.
But you built it with heavy AI assistance, and when someone asks you to write Python from
scratch on a whiteboard or in a live coding interview, you freeze.

This program fixes that.

Over 10 phases, you will rebuild a simplified version of Odibi — called **mini-odibi** — from
the ground up. Not by copying the real code. By writing every line yourself, starting from
`print("hello")` and ending with a working pipeline framework complete with tests.

The goal is not to memorize syntax. The goal is to make Python feel like a language you
*think* in. When you finish this program, you will be able to:

- Write Python fluently in interviews without hesitation
- Explain *why* your code works, not just *that* it works
- Architect solutions using the same patterns that power the real Odibi
- Debug problems by reading tracebacks instead of asking an AI

You already know more than you think. This program turns that knowledge into muscle memory.

---

## Prerequisites

You do not need much to start:

- **Python 3.9+** installed and working (`python --version` in your terminal)
- **A code editor** — VS Code is fine, so is anything else you are comfortable with
- **Jupyter notebooks** — install with `pip install jupyter` if you do not have it
- **Willingness to be uncomfortable** — some exercises will feel too easy, others will feel
  hard. Both reactions are normal. Keep going.

You do *not* need prior experience with Pandas, Pydantic, pytest, or any library. Every
phase teaches what it uses from scratch.

---

## How To Use This Program

### The Two-Pass System

Each phase has two components:

1. **Notebooks** (`notebooks/phase_XX_*.ipynb`) — Interactive lessons with explanations,
   examples, and inline exercises. Work through these first. Run every cell. Modify the
   examples. Break things on purpose and fix them.

2. **Build files** (`mini_odibi/` and `tests/`) — After completing a phase's notebook, you
   build real `.py` files that become part of mini-odibi. These are the deliverables that
   prove you learned the material.

### The Rules of Engagement

- **Work through phases in order.** Phase 5 assumes you know Phase 4. Phase 8 assumes you
  know Phases 1-7. There are no shortcuts.
- **Never skip exercises.** If a notebook says "write a function that does X," you write it.
  You do not read the solution and move on. Reading code is not the same as writing code.
- **Run everything yourself.** Every code cell, every script, every test. If you only read
  it, you did not do it.
- **Type everything.** Do not copy-paste from the notebook solutions into your build files.
  The act of typing forces your brain to process every character.
- **One phase at a time.** Do not jump ahead because a topic looks interesting. The
  progression is deliberate.

---

## Program Structure

### Phase 1: Python Basics
*The foundation everything else is built on.*

- Variables and assignment
- Data types: `int`, `float`, `str`, `bool`, `None`
- Type checking with `type()` and `isinstance()`
- Operators: arithmetic, comparison, logical
- Control flow: `if`/`elif`/`else`, `for` loops, `while` loops
- Functions: `def`, parameters, return values, default arguments, `*args`, `**kwargs`
- String formatting: f-strings, `.format()`, string methods
- Error handling: `try`/`except`/`finally`, raising exceptions, custom exception classes
- Basic I/O: `print()`, `input()`, reading/writing files

**Build deliverable:** A standalone script that reads a simple config file (plain text, not
YAML yet) and prints a formatted summary. Includes custom exceptions for missing fields.

---

### Phase 2: Data Structures and Comprehensions
*The tools you reach for every day.*

- Lists: creation, indexing, slicing, methods (`append`, `extend`, `pop`, `sort`)
- Dictionaries: creation, access, `.get()`, `.items()`, `.keys()`, `.values()`, nesting
- Sets: creation, operations (union, intersection, difference), membership testing
- Tuples: immutability, unpacking, named tuples
- List comprehensions: basic, with conditionals, nested
- Dictionary comprehensions
- Set comprehensions
- Generator expressions
- Built-in functions: `zip()`, `enumerate()`, `sorted()` with `key=lambda`, `any()`, `all()`
- Unpacking: `*args`, `**kwargs`, star expressions

**Build deliverable:** A data transformation module with functions that filter, group,
reshape, and summarize lists of dictionaries — the same kind of row-oriented data you
work with in pipelines.

---

### Phase 3: Standard Library Deep Dive
*Python's batteries-included toolkit.*

- `os` and `pathlib`: file paths, directory operations, path joining, existence checks
- `json`: loading, dumping, pretty printing, handling nested structures
- `re`: pattern matching, `search`, `match`, `findall`, `sub`, compiled patterns
- `logging`: loggers, handlers, formatters, log levels, `getLogger()`
- `datetime` and `time`: dates, timestamps, formatting, parsing, timedelta
- `hashlib` and `uuid`: hashing data for deduplication, generating unique identifiers
- `collections`: `defaultdict`, `Counter`, `OrderedDict`, `namedtuple`, `deque`
- `dataclasses`: `@dataclass`, field defaults, `__post_init__`, frozen classes
- `enum`: `Enum`, `IntEnum`, `auto()`, using enums for status codes and modes
- `contextlib`: `contextmanager`, `suppress`, writing your own context managers
- `functools`: `partial`, `lru_cache`, `reduce`, `wraps`
- `shutil`, `tempfile`, `glob`: file operations, temporary directories, pattern matching

**Build deliverable:** A file utilities module and a structured logger — the same kind of
infrastructure that supports the real Odibi under the hood.

---

### Phase 4: Object-Oriented Programming
*The architecture of real software.*

- Classes: `class`, `__init__`, `self`, instance vs. class attributes
- Methods: instance methods, `@staticmethod`, `@classmethod`
- Properties: `@property`, getters and setters, computed attributes
- Inheritance: single inheritance, `super()`, method resolution order (MRO)
- Abstract base classes: `ABC`, `@abstractmethod`, designing contracts
- Dunder methods: `__repr__`, `__str__`, `__len__`, `__eq__`, `__hash__`,
  `__getitem__`, `__contains__`, `__enter__`/`__exit__`
- Composition vs. inheritance: when to use each, has-a vs. is-a relationships
- Encapsulation: `_private` and `__mangled` naming conventions
- Class design: single responsibility, interface segregation, dependency injection

**Build deliverable:** An abstract `BaseEngine` class with a concrete `PandasEngine`
implementation. This is the beginning of mini-odibi's architecture. After completing the
Pandas engine, you build a `PolarsEngine` to reinforce the ABC pattern — proving the
abstraction works with multiple backends. Spark is covered later using mocks, matching
Odibi's real CI strategy.

---

### Phase 5: Decorators, Generators, and Advanced Patterns
*The patterns that separate beginners from professionals.*

- Decorators: what they are, how they work, writing your own
- `functools.wraps`: preserving function metadata
- Decorator factories: decorators that take arguments
- Stacking decorators
- Generators: `yield`, lazy evaluation, memory efficiency
- Generator expressions vs. list comprehensions
- Context managers: `__enter__`/`__exit__`, `@contextmanager`
- Closures: functions that capture their enclosing scope
- `lambda` functions: when to use them, when not to
- `map()`, `filter()`: functional programming basics
- The iterator protocol: `__iter__`, `__next__`

**Build deliverable:** A timing decorator, a retry decorator, and a generator-based data
reader that processes files lazily without loading everything into memory. These are
production patterns.

---

### Phase 6: Pydantic and Type Safety
*Making invalid state unrepresentable.*

- Type hints: basic annotations, `Optional`, `Union`, `List`, `Dict`, `Literal`
- Pydantic `BaseModel`: defining models, automatic validation
- `Field()`: defaults, aliases, descriptions, constraints
- Validators: `@field_validator`, `@model_validator`, custom validation logic
- Enum integration: using `Enum` fields in Pydantic models
- Model nesting: models containing other models
- Serialization: `.model_dump()`, `.model_dump_json()`
- YAML loading: reading YAML files into Pydantic models (using `pyyaml`)
- Configuration patterns: how Odibi uses Pydantic for its entire config system

**Build deliverable:** A mini-odibi configuration system — Pydantic models that define
nodes, connections, transformations, and validation rules, all loaded from YAML files.

---

### Phase 7: Pandas Deep Dive
*The engine that powers your pipelines.*

- `DataFrame` and `Series`: creation, inspection, basic operations
- Reading and writing data: CSV, JSON, Parquet, Excel
- Indexing and selection: `.loc`, `.iloc`, boolean indexing, `.query()`
- Filtering: conditions, `.isin()`, `.between()`, null handling
- Transformations: `.apply()`, `.map()`, `.assign()`, `.pipe()`
- Merging: `.merge()`, join types, merge keys, indicator columns
- Grouping: `.groupby()`, aggregation, `.transform()`, `.agg()`
- Window functions: `.rolling()`, `.expanding()`, `.shift()`, `.rank()`
- Method chaining: writing readable pipelines with `.pipe()` and `.assign()`
- String operations: `.str` accessor, pattern matching, extraction
- Date operations: `.dt` accessor, parsing dates, period calculations
- Performance: `dtypes`, `category` type, avoiding loops, vectorization

**Build deliverable:** A set of transformer functions — `rename_columns`, `filter_rows`,
`add_computed_column`, `cast_types`, `merge_dataframes` — written as standalone functions
that operate on DataFrames. These become mini-odibi's transformer library.

---

### Phase 8: Building Mini-Odibi Core
*Putting it all together — the architecture.*

- Config loader: YAML to Pydantic models, environment variable substitution
- Base engine: abstract class defining the engine contract
- Pandas engine: concrete implementation with read, write, transform operations
- Execution context: runtime state, logging, metrics tracking
- Node: the unit of work — a single table/dataset flowing through the pipeline
- Connections: local file system, with connection registry pattern
- Error handling: custom exceptions, structured error messages, graceful degradation

**Build deliverable:** A working mini-odibi core that can load a YAML config, establish
a connection, read a CSV file into a DataFrame, and write it back out. This is the
skeleton that Phase 9 adds features to.

---

### Phase 9: Building Mini-Odibi Features
*Adding the capabilities that make a framework useful.*

- Registry pattern: a dictionary-based registry for transformers, validators, patterns
- Transformers: applying named transformations from config, chaining multiple transforms
- Validation: defining data quality rules, running checks, quarantine logic
- Patterns: implementing at least one pattern (Dimension or Merge) end-to-end
- Pipeline orchestration: running nodes in sequence, handling dependencies, logging progress
- Error recovery: what to do when a node fails mid-pipeline

**Build deliverable:** A complete mini-odibi that reads a YAML config and executes a
multi-step pipeline: read source data, apply transformations, validate quality, write
output. Small enough to understand completely, powerful enough to prove you can build
real software.

---

### Phase 10: Testing and Professional Skills
*The skills that prove you are production-ready.*

- `pytest` from zero: writing your first test, running tests, reading output
- Test organization: test files, test functions, test classes, naming conventions
- Assertions: `assert`, comparing values, checking exceptions with `pytest.raises`
- Fixtures: `@pytest.fixture`, setup/teardown, scope, `conftest.py`
- Parametrize: `@pytest.mark.parametrize`, testing multiple inputs efficiently
- Mocking: `unittest.mock`, `patch`, `MagicMock`, mocking file I/O and external calls
- Test patterns: unit vs. integration, arrange-act-assert, testing edge cases
- Debugging: `breakpoint()`, `pdb`, reading tracebacks, common error patterns
- CLI basics: `argparse` or `click`, building a simple command-line interface
- Code organization: `__init__.py`, relative imports, package structure

**Build deliverable:** A full test suite for mini-odibi covering unit tests for every
module, integration tests for the pipeline, and a simple CLI that runs a pipeline from
the command line.

---

## Directory Structure

```
learning/
|
|-- README.md                          # This file — the master guide
|
|-- notebooks/                         # Interactive lessons (work through first)
|   |-- phase_01_python_basics.ipynb
|   |-- phase_02_data_structures.ipynb
|   |-- phase_03_standard_library.ipynb
|   |-- phase_04_oop.ipynb
|   |-- phase_05_advanced_patterns.ipynb
|   |-- phase_06_pydantic.ipynb
|   |-- phase_07_pandas.ipynb
|   |-- phase_08_mini_odibi_core.ipynb
|   |-- phase_09_mini_odibi_features.ipynb
|   |-- phase_10_testing.ipynb
|
|-- mini_odibi/                        # Your build — the code you write
|   |-- __init__.py
|   |-- config.py                      # Phase 6, 8: Pydantic config models
|   |-- engine/
|   |   |-- __init__.py
|   |   |-- base.py                    # Phase 4, 8: Abstract base engine
|   |   |-- pandas_engine.py           # Phase 4, 7, 8: Pandas implementation
|   |   |-- polars_engine.py           # Phase 4: Polars implementation (ABC reinforcement)
|   |-- context.py                     # Phase 8: Execution context
|   |-- node.py                        # Phase 8: Pipeline node
|   |-- connections.py                 # Phase 8: Connection registry
|   |-- transformers.py               # Phase 7, 9: Transformer functions + registry
|   |-- validation.py                  # Phase 9: Data quality checks
|   |-- patterns.py                    # Phase 9: Dimension/Merge pattern
|   |-- pipeline.py                    # Phase 9: Orchestration
|   |-- cli.py                         # Phase 10: Command-line interface
|   |-- exceptions.py                  # Phase 1, 8: Custom exceptions
|   |-- utils.py                       # Phase 3, 5: File utilities, decorators
|
|-- tests/                             # Your test suite
|   |-- __init__.py
|   |-- conftest.py                    # Shared fixtures
|   |-- test_config.py
|   |-- test_engine.py
|   |-- test_transformers.py
|   |-- test_validation.py
|   |-- test_pipeline.py
|
|-- interview_drills/                  # Quick-fire practice problems
|   |-- data_structures.py
|   |-- oop_patterns.py
|   |-- pandas_problems.py
|   |-- debugging_exercises.py
|
|-- data/                              # Sample data for exercises
|   |-- sample_config.yaml
|   |-- employees.csv
|   |-- departments.csv
```

---

## Rules

These rules exist because they work. Do not negotiate with them.

1. **Never copy-paste from the real Odibi code.** The entire point of this program is to
   build the muscle memory of writing code. If you copy it, you learned nothing. Type
   every character yourself.

2. **If you get stuck, read the real Odibi code for hints — but write your own version.**
   Look at `odibi/engine/pandas_engine.py` to understand the *approach*, then close the
   file and write your own implementation. Your version will be simpler. That is fine.
   That is the point.

3. **Every exercise must be completed before moving on.** If a notebook has 12 exercises
   and you did 10, you are not done. Go back and finish the last two. Skipping exercises
   is how gaps form, and gaps are what make you freeze in interviews.

4. **Run every code cell and script yourself.** Reading code is not doing code. You must
   see the output, see the errors, fix the errors, and see it work. Your fingers need to
   remember the keystrokes.

5. **Do not use AI to write your exercise solutions.** You can use AI to *explain* a
   concept you do not understand. You cannot use it to write the code for you. That
   defeats the purpose entirely.

6. **When you get an error, read the traceback before doing anything else.** Do not
   immediately search for help. Read the error message. Read the line number. Look at
   the code on that line. Python error messages are usually telling you exactly what is
   wrong.

---

## Interview Topics Covered

This checklist maps common Python interview topics to the phase where you learn them.
By the end of the program, every box should be checked — not because you read about it,
but because you built something with it.

| Topic | Phase | Covered By |
|---|---|---|
| Variables, types, and operators | 1 | Config script exercises |
| Control flow and loops | 1 | Config parser logic |
| Functions, `*args`, `**kwargs` | 1 | Utility functions |
| String formatting and f-strings | 1 | Output formatting |
| Error handling and custom exceptions | 1, 8 | Exception hierarchy |
| Lists, dicts, sets, tuples | 2 | Data transformation module |
| List/dict comprehensions | 2 | Data filtering and reshaping |
| `zip`, `enumerate`, `sorted` with lambda | 2 | Sorting and pairing exercises |
| File I/O and `pathlib` | 3 | File utilities module |
| JSON parsing | 3 | Config loading |
| Regular expressions | 3 | String validation |
| Logging | 3 | Structured logger |
| `datetime`, `hashlib`, `uuid` | 3 | Timestamps and hashing |
| `collections` and `dataclasses` | 3 | Counters and data holders |
| `Enum` | 3, 6 | Status codes and modes |
| Classes and `__init__` | 4 | Engine architecture |
| Inheritance and `super()` | 4 | Engine hierarchy |
| Abstract base classes | 4 | `BaseEngine` contract |
| `@property`, `@staticmethod`, `@classmethod` | 4 | Engine methods |
| Dunder methods | 4 | Node and context classes |
| Composition vs. inheritance | 4 | Pipeline design |
| Decorators (writing and using) | 5 | Timing and retry decorators |
| Generators and `yield` | 5 | Lazy data reader |
| Context managers | 5 | Resource management |
| Closures and lambda | 5 | Functional patterns |
| Type hints | 6 | All Pydantic models |
| Pydantic models and validation | 6 | Config system |
| YAML loading | 6 | Config files |
| Pandas DataFrames | 7 | Transformer functions |
| Pandas merge/join | 7 | Multi-source pipelines |
| Pandas groupby and aggregation | 7 | Aggregation transformer |
| Method chaining | 7 | Pipeline-style transforms |
| Design patterns (Registry, Strategy) | 8, 9 | Transformer and pattern registry |
| Package structure and imports | 8, 10 | mini-odibi as a package |
| `pytest` and fixtures | 10 | Full test suite |
| Mocking and `patch` | 10 | Engine and I/O mocking |
| `@pytest.mark.parametrize` | 10 | Multi-input test cases |
| CLI with `argparse`/`click` | 10 | Pipeline runner CLI |
| Debugging with `pdb`/`breakpoint()` | 10 | Debugging exercises |

---

## Estimated Timeline

This program is designed for **30 to 45 minutes per day**, consistently.

| Phase | Estimated Duration | Cumulative |
|---|---|---|
| Phase 1: Python Basics | 2-3 days | Week 1 |
| Phase 2: Data Structures | 2-3 days | Week 1 |
| Phase 3: Standard Library | 3-4 days | Week 2 |
| Phase 4: OOP | 3-4 days | Week 2 |
| Phase 5: Advanced Patterns | 2-3 days | Week 3 |
| Phase 6: Pydantic | 2-3 days | Week 3 |
| Phase 7: Pandas | 3-4 days | Week 4 |
| Phase 8: Mini-Odibi Core | 3-4 days | Week 4-5 |
| Phase 9: Mini-Odibi Features | 3-4 days | Week 5 |
| Phase 10: Testing & CLI | 3-4 days | Week 5-6 |

**Total: 4 to 6 weeks.**

Some phases will go faster if you already have partial familiarity with the topic. Others
will take longer if the material is new. Do not rush. Consistent daily practice beats
marathon sessions every time.

If you miss a day, pick up where you left off. Do not try to "make up" by doubling the
next session. That leads to burnout, not learning.

---

## How You Will Know You Are Ready

You are ready when all of the following are true:

**You can write without looking things up.**
Open a blank Python file and write a class with inheritance, a decorator, and a list
comprehension. If you can do it without checking documentation, you have internalized
the syntax.

**You can explain your choices.**
Someone asks "why did you use a dictionary here instead of a list?" and you have a
real answer — not "because the tutorial said to" but "because I need O(1) lookup by key."

**You can read a traceback and find the bug.**
A `KeyError` on line 47 does not send you to Google. You look at line 47, see the
dictionary access, check the key, and fix it in under a minute.

**You can build from scratch.**
Given a problem description — "build a system that reads config from YAML, transforms
data according to rules, and writes results" — you can architect and implement it
without AI writing the code for you. You already did this. It is called mini-odibi.

**You can whiteboard without freezing.**
Someone says "write a function that groups a list of dictionaries by a key and returns
the counts" and your hand starts moving immediately. Not because you memorized it, but
because you have written variations of it dozens of times.

**The interview drill problems feel routine.**
The exercises in `interview_drills/` no longer feel like puzzles. They feel like things
you just... do. That is fluency.

---

## One Last Thing

You already proved you can build production software. Odibi runs. It processes real data.
It solves real problems. The gap is not in your ability to think about software — it is
in your ability to express those thoughts in Python without assistance.

This program closes that gap. One phase at a time. One exercise at a time.

Start with Phase 1. Open the notebook. Write the code.
