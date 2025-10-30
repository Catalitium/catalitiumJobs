# Project Cleanup Plan

## Current Structure (Messy)
```
/catalitiumc
  app.py                    # Entry point
  app_old.py               # Backup (should be archived)
  seed.py                  # Utility script
  catalitium.db            # Database file
  jobs.csv                 # Data file
  salary.csv               # Data file
  jobs.json                # Data file
  job-titles.json          # Data file
  requirements.txt         # Dependencies
  .env                     # Config
  Procfile                 # Deployment
  fly.toml                 # Deployment
  packages.txt             # System packages
  .dockerignore            # Docker
  .gitignore               # Git
  README.md                # Docs
  ARCHITECTURE.md          # Docs
  REFACTORING_SUMMARY.md   # Docs
  /app                     # Application code
  /temp_tests              # Tests
  /.git                    # Git
  /.github                 # GitHub
```

## Target Structure (Clean)
```
/catalitiumc
  # Root level - only essential files
  README.md                # Main documentation
  requirements.txt         # Python dependencies
  .env                     # Environment config (gitignored)
  .gitignore              # Git ignore rules
  
  # Application code
  /app                     # Main application package
    __init__.py
    app.py
    /models
    /controllers
    /views
  
  # Data files
  /data                    # All data files together
    catalitium.db         # SQLite database
    jobs.csv              # Job listings
    salary.csv            # Salary reference
    jobs.json             # JSON data
    job-titles.json       # Job titles reference
  
  # Scripts and utilities
  /scripts                 # Utility scripts
    seed.py               # Database seeding
    app_old.py            # Old version backup
  
  # Tests
  /tests                   # All tests (renamed from temp_tests)
    test_search_functionality.py
    README.md
  
  # Documentation
  /docs                    # All documentation
    ARCHITECTURE.md
    REFACTORING_SUMMARY.md
    API.md (future)
  
  # Deployment configs
  /deploy                  # Deployment configurations
    Procfile              # Heroku/Render
    fly.toml              # Fly.io
    packages.txt          # System packages
    .dockerignore         # Docker ignore
  
  # CI/CD
  /.github                # GitHub Actions, etc.
  
  # Entry point
  run.py                  # Simple entry point (replaces app.py)
```

## Migration Steps

1. **Create new directories**
   - `/data`
   - `/scripts`
   - `/tests` (rename temp_tests)
   - `/docs`
   - `/deploy`

2. **Move data files**
   - `catalitium.db` → `/data/`
   - `jobs.csv` → `/data/`
   - `salary.csv` → `/data/`
   - `jobs.json` → `/data/`
   - `job-titles.json` → `/data/`

3. **Move scripts**
   - `seed.py` → `/scripts/`
   - `app_old.py` → `/scripts/backup/`

4. **Move documentation**
   - `ARCHITECTURE.md` → `/docs/`
   - `REFACTORING_SUMMARY.md` → `/docs/`

5. **Move deployment configs**
   - `Procfile` → `/deploy/`
   - `fly.toml` → `/deploy/`
   - `packages.txt` → `/deploy/`
   - `.dockerignore` → `/deploy/`

6. **Rename tests**
   - `temp_tests/` → `tests/`

7. **Update paths in code**
   - Update `DB_PATH` references
   - Update CSV file paths
   - Update import paths if needed

8. **Create simple entry point**
   - Create `run.py` as clean entry point
   - Keep `app.py` for backward compatibility

## Benefits

✅ **Clear organization** - Each directory has single purpose
✅ **Easy navigation** - Know where to find things
✅ **Cleaner root** - Only essential files visible
✅ **Better for deployment** - Configs isolated
✅ **Professional structure** - Industry standard
✅ **Easier onboarding** - New developers understand structure
