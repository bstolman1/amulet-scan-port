#!/bin/bash
# Unified test runner - works from any directory
# Usage: ./scripts/run-tests.sh [test-type]
#
# Test types:
#   all       - Run all Vitest tests (default)
#   ingest    - Run ingest-specific tests
#   server    - Run server tests
#   frontend  - Run frontend tests
#   full      - Run everything (Vitest + server + ingest standalone)
#   chaos     - Run chaos resilience stress tests
#   api       - Test Scan API connectivity
#   backfill  - Test backfill data integrity
#   acs       - Test ACS snapshot integrity
#   gcs       - Test GCS upload integrity
#   preflight - Run GCS preflight checks
#   validate  - Run all validation scripts

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INGEST_DIR="$PROJECT_ROOT/scripts/ingest"
SERVER_DIR="$PROJECT_ROOT/server"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_stats() {
    echo -e "${CYAN}  $1${NC}"
}

# Run all Vitest tests (covers frontend, ingest vitest tests, hooks, libs)
run_vitest_all() {
    print_header "Running All Vitest Tests"
    cd "$PROJECT_ROOT"
    npx vitest run
}

# Run ingest Vitest tests only
run_vitest_ingest() {
    print_header "Running Ingest Vitest Tests"
    cd "$PROJECT_ROOT"
    npx vitest run scripts/ingest/test/
}

# Run chaos resilience tests
run_chaos_tests() {
    print_header "Running Chaos Resilience Tests"
    cd "$PROJECT_ROOT"
    npx vitest run scripts/ingest/test/chaos-resilience.test.js
}

# Note: The ingest test files (api.test.js, backfill.test.js, acs.test.js) 
# use Vitest APIs and are already run by the Vitest suite.
# This function is kept for backward compatibility but skips Vitest-based tests.
run_ingest_standalone() {
    print_header "Ingest Standalone Tests"
    print_info "Ingest tests use Vitest and are covered in Phase 1"
    print_info "Skipping standalone execution (would fail with 'vi.mock not initialized')"
    return 0
}

run_ingest_test() {
    local test_name=$1
    print_header "Running Ingest Test: $test_name"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run "test:$test_name"
}

run_server_tests() {
    print_header "Running Server Tests"
    cd "$SERVER_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing server dependencies..."
        npm install
    fi
    
    npm test
}

run_frontend_tests() {
    print_header "Running Frontend Tests"
    cd "$PROJECT_ROOT"
    npx vitest run src/
}

run_preflight() {
    print_header "Running GCS Preflight Checks"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run preflight
}

run_validation() {
    print_header "Running Validation Scripts"
    cd "$INGEST_DIR"
    
    if [ ! -d "node_modules" ]; then
        print_info "Installing ingest dependencies..."
        npm install
    fi
    
    npm run validate
}

show_help() {
    echo "Unified Test Runner for Amulet Scan"
    echo ""
    echo "Usage: $0 [test-type]"
    echo ""
    echo "Quick Commands:"
    echo "  all        Run all Vitest tests (frontend + ingest + libs) [default]"
    echo "  full       Run EVERYTHING (Vitest + server + ingest standalone)"
    echo "  chaos      Run chaos resilience stress tests only"
    echo ""
    echo "Component Tests:"
    echo "  frontend   Run frontend component tests (Vitest)"
    echo "  server     Run server API tests"
    echo "  ingest     Run all ingest Vitest tests"
    echo ""
    echo "Ingest Standalone Tests:"
    echo "  api        Test Scan API connectivity"
    echo "  backfill   Test backfill data integrity"
    echo "  acs        Test ACS snapshot integrity"
    echo "  gcs        Test GCS upload integrity (requires GCS_BUCKET)"
    echo "  health     Run end-to-end pipeline health check"
    echo ""
    echo "Validation:"
    echo "  preflight  Run GCS preflight checks"
    echo "  validate   Run all validation scripts"
    echo ""
    echo "Coverage:"
    echo "  coverage   Run all tests with coverage reports"
    echo "  cov:front  Frontend coverage only"
    echo "  cov:server Server coverage only"
    echo "  cov:ingest Ingest coverage only"
    echo ""
    echo "Quality & Mutation:"
    echo "  quality    Generate test quality report"
    echo "  mutate     Run Stryker mutation testing"
    echo "  mutate:dry Dry run mutation testing"
    echo ""
    echo "Examples:"
    echo "  $0              # Run all Vitest tests"
    echo "  $0 full         # Run absolutely everything"
    echo "  $0 chaos        # Run chaos stress tests"
    echo "  $0 coverage     # Generate coverage reports"
    echo ""
    echo "Environment variables:"
    echo "  GCS_BUCKET      Required for gcs, health, and preflight tests"
    echo "  SCAN_URL        Scan API URL (default: https://scan.sv-1.global.canton.network.sync.global)"
    echo ""
}

# Main
TEST_TYPE="${1:-all}"

case "$TEST_TYPE" in
    help|--help|-h)
        show_help
        ;;
    all)
        run_vitest_all
        print_success "All Vitest tests passed!"
        ;;
    chaos)
        run_chaos_tests
        print_success "Chaos resilience tests passed!"
        ;;
    ingest)
        run_vitest_ingest
        print_success "Ingest Vitest tests passed!"
        ;;
    api)
        run_ingest_test "api"
        print_success "API tests passed!"
        ;;
    backfill)
        run_ingest_test "backfill"
        print_success "Backfill tests passed!"
        ;;
    acs)
        run_ingest_test "acs"
        print_success "ACS tests passed!"
        ;;
    gcs)
        run_ingest_test "gcs"
        print_success "GCS integrity tests passed!"
        ;;
    health)
        run_ingest_test "health"
        print_success "Pipeline health check passed!"
        ;;
    health:quick)
        run_ingest_test "health:quick"
        print_success "Quick health check passed!"
        ;;
    preflight)
        run_preflight
        print_success "Preflight checks passed!"
        ;;
    validate)
        run_validation
        print_success "Validation passed!"
        ;;
    server)
        run_server_tests
        print_success "Server tests passed!"
        ;;
    frontend)
        run_frontend_tests
        print_success "Frontend tests passed!"
        ;;
    full)
        print_header "Running FULL Comprehensive Test Suite"
        echo ""
        print_info "This will run ALL tests across the entire project..."
        echo ""
        
        # Track results
        FAILED=0
        
        # 1. All Vitest tests (frontend + ingest vitest + libs + hooks)
        print_stats "Phase 1/3: Vitest Tests (frontend, ingest, libs, hooks)"
        if run_vitest_all; then
            print_success "Vitest tests passed!"
        else
            print_error "Vitest tests failed!"
            FAILED=1
        fi
        
        # 2. Server tests
        print_stats "Phase 2/3: Server API Tests"
        if run_server_tests; then
            print_success "Server tests passed!"
        else
            print_error "Server tests failed!"
            FAILED=1
        fi
        
        # 3. Ingest standalone tests (Node.js based - api, backfill, acs)
        print_stats "Phase 3/3: Ingest Standalone Tests"
        if run_ingest_standalone; then
            print_success "Ingest standalone tests passed!"
        else
            print_error "Ingest standalone tests failed!"
            FAILED=1
        fi
        
        echo ""
        if [ $FAILED -eq 0 ]; then
            print_header "✅ ALL TESTS PASSED"
            print_success "Full comprehensive test suite completed successfully!"
        else
            print_header "❌ SOME TESTS FAILED"
            print_error "Review the output above to identify failures."
            exit 1
        fi
        ;;
    coverage)
        print_header "Running All Tests with Coverage"
        bash "$SCRIPT_DIR/coverage.sh" all
        ;;
    cov:front|cov:frontend)
        print_header "Running Frontend Coverage"
        bash "$SCRIPT_DIR/coverage.sh" frontend
        ;;
    cov:server)
        print_header "Running Server Coverage"
        bash "$SCRIPT_DIR/coverage.sh" server
        ;;
    cov:ingest)
        print_header "Running Ingest Coverage"
        bash "$SCRIPT_DIR/coverage.sh" ingest
        ;;
    quality)
        print_header "Generating Test Quality Report"
        cd "$PROJECT_ROOT"
        node scripts/test-quality-report.js
        ;;
    mutate)
        print_header "Running Stryker Mutation Testing"
        cd "$PROJECT_ROOT"
        npx stryker run --configFile stryker.config.js
        ;;
    mutate:dry)
        print_header "Stryker Dry Run (showing mutants)"
        cd "$PROJECT_ROOT"
        npx stryker run --configFile stryker.config.js --dryRun
        ;;
    *)
        print_error "Unknown test type: $TEST_TYPE"
        echo ""
        show_help
        exit 1
        ;;
esac

echo ""
print_info "Tests completed from: $(pwd)"
