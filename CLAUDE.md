# psync - Claude Development Guidelines

## Communication Style
- Always ask questions and present options to the CTO before major decisions
- Use AskUserQuestion tool with clear options for architectural choices
- Don't assume - clarify requirements upfront

## Code Style (tinygrad-inspired)
- Terse, dense, functional
- Favor comprehensions and one-liners where readable
- Minimal classes - use NamedTuple, dataclass with slots
- No unnecessary abstractions
- Type hints as documentation, minimal docstrings

## Architecture
- Single-file design: `psync.py` with PEP 723 metadata
- Dependencies: `zstandard`, `xxhash` only
- Python 3.11+ required

## Key Algorithms
- Rolling checksum: Adler-32 variant with O(1) roll
- Strong hash: xxh3_128 (~30GB/s)
- Block sizing: Tiered 128KB→1GB based on file size
- Fast path: Skip delta if size+mtime unchanged

## Testing
- Use `pv` (pipe viewer) to simulate slow network
- Test with generated files of various sizes
- Run sender/receiver over pipes, not SSH, for unit tests

## Commands
```bash
uv run psync.py src/ dst/              # local sync
uv run psync.py src/ host:/path/       # remote sync
uv run psync.py --server               # server mode (internal)
```

## Don't
- Add external dependencies beyond zstandard/xxhash
- Create multiple files - keep it single-file
- Over-engineer or add unused abstractions
- Ignore the fast path optimization
