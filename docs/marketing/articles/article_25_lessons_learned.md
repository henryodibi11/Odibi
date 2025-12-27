# What I Learned Building an Open Source Data Framework

*Reflections on a year of building in public*

---

## TL;DR

Building Odibi taught me more than technical skills. It taught me about constraints driving creativity, the value of documentation, the importance of real-world testing, and the power of community. This article shares the lessons learned-both technical and personal.

---

## Why I Built It

I'm a solo data engineer on an analytics team. No IT support for my specific needs. No budget for enterprise tools. Just me, a laptop, and problems to solve.

Every pipeline I built followed the same patterns:
- SCD2 dimensions
- Fact tables with lookups
- Data quality contracts
- Medallion architecture

I was writing the same code over and over. Different projects, same patterns.

So I extracted the patterns into a framework. Not because I wanted to build a product-because I was tired of repeating myself.

That's how Odibi started. A tool to solve my own problems.

---

## Lesson 1: Constraints Drive Creativity

I couldn't access production databases directly. I had to work with CSV exports and API calls.

At first, I saw this as a limitation. Now I see it as a feature.

Because I couldn't rely on database features, I built:
- Engine-agnostic patterns (Spark, Pandas, Polars)
- File-based state management
- Configuration-driven everything

The constraints forced better architecture.

### The Takeaway

Don't fight constraints. Work with them. They often lead to better solutions.

---

## Lesson 2: Documentation Is the Product

Early versions of Odibi had minimal docs. "The code is self-explanatory."

Nobody used it.

Then I wrote the YAML reference. Then the pattern guides. Then the tutorials.

Usage jumped. Not because the code changed-because people could finally understand it.

### What I Learned

- Good docs = more users
- Bad docs = great code nobody uses
- Docstrings are the source of truth
- Auto-generate where possible
- Examples matter more than explanations

### The Docstring Pattern

```python
def dimension_pattern(
    df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    scd_type: int = 1,
    **kwargs
) -> DataFrame:
    """
    Build a dimension table with surrogate key generation.
    
    Parameters
    ----------
    df : DataFrame
        Input data with dimension attributes
    natural_key : str
        Business key column (e.g., 'customer_id')
    surrogate_key : str
        Name of surrogate key to generate (e.g., 'customer_sk')
    scd_type : int
        0=static, 1=overwrite, 2=history tracking
    
    Returns
    -------
    DataFrame
        Dimension table with surrogate key
    
    Example
    -------
    >>> result = dimension_pattern(
    ...     df=customers,
    ...     natural_key='customer_id',
    ...     surrogate_key='customer_sk',
    ...     scd_type=2
    ... )
    """
```

---

## Lesson 3: Real-World Testing Reveals Everything

Unit tests passed. Integration tests passed. Production failed.

Why? Because real data is weird:
- NULLs where you don't expect them
- Unicode characters that break parsing
- Timestamps in unexpected formats
- Values that exceed reasonable bounds

### What I Changed

1. **Test with production-like data**: Not just happy path
2. **Add chaos**: Inject NULLs, duplicates, out-of-range values
3. **Fuzz testing**: Random data reveals edge cases
4. **Production monitoring**: Track what actually fails

### The Testing Pyramid

```
         /\
        /  \
       / E2E \        ← Few: Full pipeline tests
      /--------\
     /          \
    / Integration \   ← Some: Component tests
   /--------------\
  /                \
 /    Unit Tests    \ ← Many: Function-level tests
/--------------------\
```

---

## Lesson 4: YAML Is a User Interface

Configuration is user experience. Bad YAML = bad UX.

Early versions were verbose:

```yaml
# Old: Too verbose
transform:
  steps:
    - type: function
      name: clean_text
      parameters:
        columns:
          - name: customer_name
            case: upper
            trim: true
          - name: customer_city
            case: upper
            trim: true
```

Current version is cleaner:

```yaml
# New: Readable
transform:
  steps:
    - function: clean_text
      params:
        columns: [customer_name, customer_city]
        case: upper
        trim: true
```

### YAML Design Principles

1. **Minimize nesting**: 3 levels max
2. **Sensible defaults**: Only specify what differs
3. **Consistent naming**: `params`, not `parameters` or `config`
4. **Clear errors**: Show exactly what's wrong

---

## Lesson 5: Start With One Engine, Expand Later

I wanted to support Spark, Pandas, Polars, and DuckDB from day one.

Mistake.

Each engine has quirks:
- Spark: Lazy evaluation, distributed
- Pandas: Eager, single-node
- Polars: Lazy by default, different API
- DuckDB: SQL-first

I spent weeks on compatibility instead of features.

### Better Approach

1. **Pick one engine**: I chose Pandas for dev speed
2. **Build all patterns**: Get it working first
3. **Add engines incrementally**: Spark second, Polars third
4. **Abstract at boundaries**: Engine-specific code isolated

---

## Lesson 6: Patterns Over Features

I kept wanting to add features:
- Support for streaming
- Real-time ingestion
- ML model serving
- ...

But the core patterns weren't solid yet.

### What I Focused On Instead

1. **Dimension pattern**: Works perfectly
2. **Fact pattern**: Handles all edge cases
3. **SCD2**: Robust and tested
4. **Contracts**: Catches real issues

Features without solid patterns = shaky foundation.

---

## Lesson 7: Build for Yourself First

Some advice says "talk to users before building."

I was my own user. I knew exactly what I needed:
- Less boilerplate
- Declarative configuration
- Patterns I could trust

Only after solving my problems did I share it.

### The Result

The framework works because it solves real problems I actually had. Not theoretical problems. Real ones.

---

## Lesson 8: Community Matters

When I shared Odibi publicly:
- Strangers filed bug reports
- Contributors suggested improvements
- Users shared use cases I hadn't imagined

The framework improved faster with community input than it ever did in isolation.

### What Helped

- Clear contribution guide
- Responsive to issues
- Public roadmap
- Welcoming attitude

---

## Lesson 9: Incremental Progress Compounds

I couldn't work on Odibi full-time. Maybe an hour a day. Sometimes less.

But an hour a day, every day, for a year = 365 hours.

That's enough to build something real.

### The Compounding Effect

| Month | Cumulative Hours | State |
|-------|------------------|-------|
| 1 | 30 | Basic structure |
| 3 | 90 | Core patterns working |
| 6 | 180 | Documentation complete |
| 12 | 365 | Production-ready |

Small, consistent effort beats sporadic bursts.

---

## Lesson 10: Sharing Is Learning

Writing this article series forced me to:
- Organize my knowledge
- Fill gaps in my understanding
- Explain things clearly

Teaching is learning.

Every article I wrote, I learned something new about my own work.

---

## What I'd Do Differently

### Start With Tests

I added tests after the fact. Some patterns were hard to test because they weren't designed for it.

**Next time**: Test-first design.

### Document As I Build

I documented after building. Details were forgotten.

**Next time**: Docstrings before implementation.

### Release Earlier

I waited until it was "ready." Ready is never.

**Next time**: Release as soon as it solves one problem.

---

## Advice for Solo Builders

1. **Solve your own problem first**: You understand it best
2. **Document obsessively**: Future you (and others) will thank you
3. **Test with real data**: Happy path tests lie
4. **Share early**: Feedback is gold
5. **Compound progress**: Small daily effort adds up
6. **Embrace constraints**: They force creativity

---

## What's Next

Odibi is far from done. The roadmap includes:
- Streaming support
- More engine integrations
- Better error messages
- Visual pipeline designer

But the core is solid. It solves real problems. It's used in production.

That's enough for now.

---

## Thank You

To everyone who:
- Read these articles
- Filed issues
- Contributed code
- Shared feedback

You made this better.

---

## Final Thoughts

Building in public is scary. Your mistakes are visible. Your progress is slow. Your code is imperfect.

But the alternative-building in private-means no feedback, no community, no improvement.

I'd rather build imperfectly in public than perfectly in private.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series

---

*Next article: The final summary-a complete index of everything we covered.*
