# Catalitium Architecture Documentation

## Overview
Catalitium is a job search platform with salary insights, built with Flask using a clean MVC architecture.

## Directory Structure

```
/catalitium
  /app                      # Main application package
    __init__.py            # Package marker
    app.py                 # Flask application factory
    /models                # DATA LAYER
      __init__.py
      db.py                # Database connections, data parsing, filtering
    /controllers           # BUSINESS LOGIC LAYER
      __init__.py
      main_controller.py   # Route handlers (Blueprint)
    /views                 # PRESENTATION LAYER
      __init__.py
      /templates
        base.html          # Base template with layout
        index.html         # Search results page
        admin_metrics.html # Admin analytics
  requirements.txt        # Python dependencies
  .env                    # Environment variables (not in git)
```

## Core Components

### 1. Application Factory (`app/app.py`)
**Purpose**: Creates and configures the Flask application

**Key Functions**:
- `create_app()` - Initializes Flask app, registers blueprints, sets config

**Configuration**:
- `SECRET_KEY` - Flask session security (required)
- `GTM_CONTAINER_ID` - Google Tag Manager ID
- `PER_PAGE_MAX` - Maximum results per page (100)

### 2. Models Layer (`app/models/db.py`)
**Purpose**: Data access, parsing, and business logic

#### Database Functions
- `get_db()` - Get database connection (psycopg, cached per request)
- `close_db()` - Close database connection at teardown
- `init_db()` - Ensure required tables exist in Postgres
- `_pg_connect()` - Connect to PostgreSQL


#### Search & Filtering
- `normalize_title()` - Standardize job titles (e.g., "SWE" → "software engineer")
- `normalize_country()` - Standardize country codes (e.g., "germany" → "DE")
- `parse_salary_query()` - Parse salary filters from search (e.g., "80k-120k", ">100k")

#### Analytics
- `log_search()` - Log basic search query
- `log_search_event()` - Log detailed search analytics
- `log_job_view_event()` - Log job view events
- `_hash()` - Hash sensitive data for privacy
- `_client_meta()` - Extract client metadata (IP, user agent, etc.)

#### Utilities
- `_fuzzy_match()` - Token-based text matching
- `parse_money_numbers()` - Extract salary numbers from text
- `extract_country_code()` - Extract country from location string

### 3. Controllers Layer (`app/controllers/main_controller.py`)
**Purpose**: Handle HTTP requests and responses

**Blueprint**: `main_bp` - All routes registered under this blueprint

#### Routes
- `GET /` (`index`) - Main search page
  - Query params: `title`, `country`, `page`, `per_page`
  - Returns: Filtered and paginated job results
  
- `POST /subscribe` (`subscribe`) - Email subscription
  - Rate limited: 5/minute, 50/hour
  - Validates email with RFC compliance
  
- `POST /events/job_view` (`events_job_view`) - Analytics endpoint
  - Logs when users view job details
  
- `GET /admin/metrics` (`admin_metrics`) - Admin dashboard
  - Requires `ADMIN_TOKEN` query parameter
  
- `GET /api/salary-insights` (`api_salary_insights`) - API endpoint
  - Returns salary data for jobs matching filters

### 4. Views Layer (`app/views/templates/`)
**Purpose**: HTML templates for rendering

- `base.html` - Base layout with header, footer, navigation
- `index.html` - Search form and results display
- `admin_metrics.html` - Analytics dashboard

**Template Variables**:
- `results` - List of job dictionaries
- `count` - Total number of results
- `title_q` - Normalized title query
- `country_q` - Normalized country query
- `pagination` - Pagination metadata

## Data Flow

### Search Request Flow
```
1. User enters search -> GET /?title=engineer&country=de
2. main_controller.index() receives request
3. Parse salary from title -> parse_salary_query()
4. Normalize inputs -> normalize_title(), normalize_country()
5. Fetch jobs from Postgres -> Job.search()
6. Shape rows for the template (format dates, sanitize links)
7. Log analytics -> log_search(), log_search_event()
8. Render template -> render_template("index.html")
```

### Database Connection Flow
```
1. Request starts -> get_db() called
2. get_db() opens a psycopg connection (autocommit)
3. Connection is cached on flask.g for the duration of the request
4. Request ends -> close_db() closes the connection
```

## Environment Variables

Required:
- `SECRET_KEY` - Flask secret key (must be set, no default)

Optional:
- `DATABASE_URL` - PostgreSQL connection string
- `SUPABASE_URL` - Alternative Postgres URL (legacy)
- `GTM_CONTAINER_ID` - Google Tag Manager ID
- `ADMIN_TOKEN` - Token for admin access
- `RATELIMIT_STORAGE_URL` - Rate limit storage (default: memory://)

## Database Schema

Managed through Postgres migrations (Jobs, subscribers).

## Caching Strategy

In-memory caching has been removed in favor of querying Postgres directly.


- **Location**: `_jobs_cache` dictionary in memory
- **Key**: File path + modification time
- **Invalidation**: Automatic on file change
- **Refresh**: On-demand when file mtime changes

- **Location**: `_salary_cache` dictionary in memory
- **Key**: File path + modification time
- **Invalidation**: Automatic on file change
- **Refresh**: Automatically when source file modification time changes

## Error Handling

### Database Errors
- Postgres connection failures -> surfaced and logged during startup
- Duplicate email subscriptions -> Gracefully handled with success message

### User Input Errors
- Invalid email -> Flash error message, redirect
- Invalid search params -> Treated as empty search
- Out of range pagination -> Clamped to valid range

## Security Features


1. **Secret Key Enforcement**: App refuses to start without valid SECRET_KEY
2. **Rate Limiting**: Subscribe endpoint limited to 5/min, 50/hour
3. **Email Validation**: RFC-compliant validation via email-validator
4. **SQL Injection Protection**: Parameterized queries throughout
5. **Privacy**: IP addresses and emails hashed before storage
6. **CSRF Protection**: Flask session management
7. **Admin Access**: Token-based authentication for metrics

## Performance Optimizations

1. Postgres indexes on frequently queried columns
2. Server-side pagination to limit result size
3. Autocommit connections to avoid long-running transactions

## Testing


See `temp_tests/` for functional tests covering:
- Search functionality
- Query parsing
- Data filtering
- Pagination
- Database connections
- Subscription flow

Run tests: `pytest temp_tests/test_search_functionality.py -v`

## Known Limitations

1. No authentication: Admin endpoint uses simple token
2. Limited search: No full-text search, simple filters only
3. Analytics tables can grow without archival policies

## Future Improvements


1. Move to proper database (PostgreSQL) for jobs data
2. Add Elasticsearch for advanced search
3. Implement user accounts and saved searches
4. Add email notification system
5. API rate limiting and authentication
6. Comprehensive test coverage
7. Docker containerization
8. CI/CD pipeline







